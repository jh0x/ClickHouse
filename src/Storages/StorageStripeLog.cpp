#include <sys/types.h>

#include <optional>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>

#include <Core/Settings.h>

#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>

#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>

#include <DataTypes/DataTypeFactory.h>

#include <Interpreters/Context.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageStripeLog.h>
#include <Storages/StorageLogSettings.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>

#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupEntryFromSmallFile.h>
#include <Backups/BackupEntryWrappedWith.h>
#include <Backups/IBackup.h>
#include <Backups/RestorerFromBackup.h>
#include <Disks/TemporaryFileOnDisk.h>

#include <base/insertAtEnd.h>

#include <cassert>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_compress_block_size;
    extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
    extern const int TIMEOUT_EXCEEDED;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int NOT_IMPLEMENTED;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char stripe_log_sink_write_fallpoint[];
}

/// NOTE: The lock `StorageStripeLog::rwlock` is NOT kept locked while reading,
/// because we read ranges of data that do not change.
class StripeLogSource final : public ISource
{
public:
    static Block getHeader(
        const StorageSnapshotPtr & storage_snapshot,
        const Names & column_names,
        IndexForNativeFormat::Blocks::const_iterator index_begin,
        IndexForNativeFormat::Blocks::const_iterator index_end)
    {
        if (index_begin == index_end)
            return storage_snapshot->getSampleBlockForColumns(column_names);

        /// TODO: check if possible to always return storage.getSampleBlock()

        Block header;

        for (const auto & column : index_begin->columns)
        {
            auto type = DataTypeFactory::instance().get(column.type);
            header.insert(ColumnWithTypeAndName{ type, column.name });
        }

        return header;
    }

    StripeLogSource(
        const StorageStripeLog & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const Names & column_names,
        ReadSettings read_settings_,
        std::shared_ptr<const IndexForNativeFormat> indices_,
        IndexForNativeFormat::Blocks::const_iterator index_begin_,
        IndexForNativeFormat::Blocks::const_iterator index_end_,
        size_t file_size_)
        : ISource(std::make_shared<const Block>(getHeader(storage_snapshot_, column_names, index_begin_, index_end_)))
        , storage(storage_)
        , storage_snapshot(storage_snapshot_)
        , read_settings(std::move(read_settings_))
        , indices(indices_)
        , index_begin(index_begin_)
        , index_end(index_end_)
        , file_size(file_size_)
    {
    }

    String getName() const override { return "StripeLog"; }

protected:
    Chunk generate() override
    {
        Block res;
        start();

        if (block_in)
        {
            res = block_in->read();

            /// Freeing memory before destroying the object.
            if (res.empty())
            {
                block_in.reset();
                data_in.reset();
                indices.reset();
            }
        }

        return Chunk(res.getColumns(), res.rows());
    }

private:
    const StorageStripeLog & storage;
    StorageSnapshotPtr storage_snapshot;
    ReadSettings read_settings;

    std::shared_ptr<const IndexForNativeFormat> indices;
    IndexForNativeFormat::Blocks::const_iterator index_begin;
    IndexForNativeFormat::Blocks::const_iterator index_end;
    size_t file_size;

    Block header;

    /** optional - to create objects only on first reading
      *  and delete objects (release buffers) after the source is exhausted
      * - to save RAM when using a large number of sources.
      */
    bool started = false;
    std::optional<CompressedReadBufferFromFile> data_in;
    std::optional<NativeReader> block_in;

    void start()
    {
        if (!started)
        {
            started = true;

            String data_file_path = storage.table_path + "data.bin";
            data_in.emplace(storage.disk->readFile(data_file_path, read_settings.adjustBufferSize(file_size)));
            block_in.emplace(*data_in, 0, index_begin, index_end);
        }
    }
};


/// NOTE: The lock `StorageStripeLog::rwlock` is kept locked in exclusive mode while writing.
class StripeLogSink final : public SinkToStorage
{
public:
    using WriteLock = std::unique_lock<std::shared_timed_mutex>;

    explicit StripeLogSink(StorageStripeLog & storage_, const StorageMetadataPtr & metadata_snapshot_, WriteLock && lock_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , lock(std::move(lock_))
        , data_out_compressed(storage.disk->writeFile(storage.data_file_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append))
        , data_out(std::make_unique<CompressedWriteBuffer>(
              *data_out_compressed, CompressionCodecFactory::instance().getDefaultCodec(), storage.max_compress_block_size))
    {
        if (!lock)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

        /// Ensure that indices are loaded because we're going to update them.
        storage.loadIndices(lock);

        /// If there were no files, save zero file sizes to be able to rollback in case of error.
        storage.saveFileSizes(lock);

        size_t initial_data_size = storage.file_checker.getFileSize(storage.data_file_path);
        block_out = std::make_unique<NativeWriter>(*data_out, 0, std::make_shared<const Block>(metadata_snapshot->getSampleBlock()), std::nullopt, false, &storage.indices, initial_data_size);
    }

    String getName() const override { return "StripeLogSink"; }

    ~StripeLogSink() override
    {
        try
        {
            if (!done)
            {
                /// Rollback partial writes.

                /// No more writing.
                data_out->cancel();
                data_out.reset();

                data_out_compressed->cancel();
                data_out_compressed.reset();

                /// Truncate files to the older sizes.
                storage.file_checker.repair();

                /// Remove excessive indices.
                storage.removeUnsavedIndices(lock);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void consume(Chunk & chunk) override
    {
        block_out->write(getHeader().cloneWithColumns(chunk.getColumns()));
    }

    void onFinish() override
    {
        if (done)
            return;

        data_out->finalize();
        data_out_compressed->finalize();

        /// Save the new indices.
        storage.saveIndices(lock);

        // While executing save file sizes the exception might occurs. S3::TooManyRequests for example.
        fiu_do_on(FailPoints::stripe_log_sink_write_fallpoint,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault for inserting into StipeLog table");
        });
        /// Save the new file sizes.
        storage.saveFileSizes(lock);

        storage.updateTotalRows(lock);

        done = true;

        /// unlock should be done from the same thread as lock, and dtor may be
        /// called from different thread, so it should be done here (at least in
        /// case of no exceptions occurred)
        lock.unlock();
    }

private:
    StorageStripeLog & storage;
    StorageMetadataPtr metadata_snapshot;
    WriteLock lock;

    std::unique_ptr<WriteBuffer> data_out_compressed;
    std::unique_ptr<CompressedWriteBuffer> data_out;
    std::unique_ptr<NativeWriter> block_out;

    bool done = false;
};


StorageStripeLog::StorageStripeLog(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    LoadingStrictnessLevel mode,
    ContextMutablePtr context_)
    : IStorage(table_id_)
    , WithMutableContext(context_)
    , disk(std::move(disk_))
    , table_path(relative_path_)
    , data_file_path(table_path + "data.bin")
    , index_file_path(table_path + "index.mrk")
    , file_checker(disk, table_path + "sizes.json")
    , max_compress_block_size(context_->getSettingsRef()[Setting::max_compress_block_size])
    , log(getLogger("StorageStripeLog"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    if (relative_path_.empty())
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Storage {} requires data path", getName());

    /// Ensure the file checker is initialized.
    if (file_checker.empty())
    {
        file_checker.setEmpty(data_file_path);
        file_checker.setEmpty(index_file_path);
    }

    if (mode < LoadingStrictnessLevel::ATTACH)
    {
        /// create directories if they do not exist
        disk->createDirectories(table_path);
    }
    else
    {
        try
        {
            file_checker.repair();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    total_bytes = file_checker.getTotalSize();
}


StorageStripeLog::~StorageStripeLog() = default;


void StorageStripeLog::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    assert(table_path != new_path_to_table_data);
    {
        disk->createDirectories(new_path_to_table_data);
        disk->moveDirectory(table_path, new_path_to_table_data);

        table_path = new_path_to_table_data;
        data_file_path = table_path + "data.bin";
        index_file_path = table_path + "index.mrk";
        file_checker.setPath(table_path + "sizes.json");
    }
    renameInMemory(new_table_id);
}


static std::chrono::seconds getLockTimeout(ContextPtr local_context)
{
    const Settings & settings = local_context->getSettingsRef();
    Int64 lock_timeout = settings[Setting::lock_acquire_timeout].totalSeconds();
    if (settings[Setting::max_execution_time].totalSeconds() != 0 && settings[Setting::max_execution_time].totalSeconds() < lock_timeout)
        lock_timeout = settings[Setting::max_execution_time].totalSeconds();
    return std::chrono::seconds{lock_timeout};
}


Pipe StorageStripeLog::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    auto lock_timeout = getLockTimeout(local_context);
    loadIndices(lock_timeout);

    ReadLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    size_t data_file_size = file_checker.getFileSize(data_file_path);
    if (!data_file_size)
        return Pipe(std::make_shared<NullSource>(std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names))));

    auto indices_for_selected_columns
        = std::make_shared<IndexForNativeFormat>(indices.extractIndexForColumns(NameSet{column_names.begin(), column_names.end()}));

    size_t size = indices_for_selected_columns->blocks.size();
    num_streams = std::min(num_streams, size);

    ReadSettings read_settings = local_context->getReadSettings();
    Pipes pipes;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        IndexForNativeFormat::Blocks::const_iterator begin = indices_for_selected_columns->blocks.begin();
        IndexForNativeFormat::Blocks::const_iterator end = indices_for_selected_columns->blocks.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        pipes.emplace_back(std::make_shared<StripeLogSource>(
            *this, storage_snapshot, column_names, read_settings, indices_for_selected_columns, begin, end, data_file_size));
    }

    /// We do not keep read lock directly at the time of reading, because we read ranges of data that do not change.

    return Pipe::unitePipes(std::move(pipes));
}


SinkToStoragePtr StorageStripeLog::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    WriteLock lock{rwlock, getLockTimeout(local_context)};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    return std::make_shared<StripeLogSink>(*this, metadata_snapshot, std::move(lock));
}

IStorage::DataValidationTasksPtr StorageStripeLog::getCheckTaskList(
    const std::variant<std::monostate, ASTPtr, String> & check_task_filter, ContextPtr local_context)
{
    if (!std::holds_alternative<std::monostate>(check_task_filter))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CHECK PART/PARTITION are not supported for {}", getName());

    ReadLock lock{rwlock, getLockTimeout(local_context)};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");
    return std::make_unique<DataValidationTasks>(file_checker.getDataValidationTasks(), std::move(lock));
}

std::optional<CheckResult> StorageStripeLog::checkDataNext(DataValidationTasksPtr & check_task_list)
{
    return file_checker.checkNextEntry(assert_cast<DataValidationTasks *>(check_task_list.get())->file_checker_tasks);
}

void StorageStripeLog::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    disk->clearDirectory(table_path);

    indices.clear();
    file_checker.setEmpty(data_file_path);
    file_checker.setEmpty(index_file_path);

    indices_loaded = true;
    num_indices_saved = 0;
    total_rows = 0;
    total_bytes = 0;
    getContext()->clearMMappedFileCache();
}


void StorageStripeLog::loadIndices(std::chrono::seconds lock_timeout)
{
    if (indices_loaded)
        return;

    /// We load indices with an exclusive lock (i.e. the write lock) because we don't want
    /// a data race between two threads trying to load indices simultaneously.
    WriteLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    loadIndices(lock);
}


void StorageStripeLog::loadIndices(const WriteLock & lock /* already locked exclusively */)
{
    if (indices_loaded)
        return;

    if (disk->existsFile(index_file_path))
    {
        CompressedReadBufferFromFile index_in(disk->readFile(index_file_path, getContext()->getReadSettings().adjustBufferSize(4096)));
        indices.read(index_in);
    }

    indices_loaded = true;
    num_indices_saved = indices.blocks.size();

    /// We need indices to calculate the number of rows, and now we have the indices.
    updateTotalRows(lock);
}


void StorageStripeLog::saveIndices(const WriteLock & /* already locked for writing */)
{
    size_t num_indices = indices.blocks.size();
    if (num_indices_saved == num_indices)
        return;

    size_t start = num_indices_saved;
    auto index_out_compressed = disk->writeFile(index_file_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
    auto index_out = std::make_unique<CompressedWriteBuffer>(*index_out_compressed);

    for (size_t i = start; i != num_indices; ++i)
        indices.blocks[i].write(*index_out);

    index_out->finalize();
    index_out_compressed->finalize();

    num_indices_saved = num_indices;
}


void StorageStripeLog::removeUnsavedIndices(const WriteLock & /* already locked for writing */)
{
    if (indices.blocks.size() > num_indices_saved)
        indices.blocks.resize(num_indices_saved);
}


void StorageStripeLog::saveFileSizes(const WriteLock & /* already locked for writing */)
{
    file_checker.update(data_file_path);
    file_checker.update(index_file_path);
    file_checker.save();
    total_bytes = file_checker.getTotalSize();
}


void StorageStripeLog::updateTotalRows(const WriteLock &)
{
    if (!indices_loaded)
        return;

    size_t new_total_rows = 0;
    for (const auto & block : indices.blocks)
        new_total_rows += block.num_rows;
    total_rows = new_total_rows;
}

std::optional<UInt64> StorageStripeLog::totalRows(ContextPtr) const
{
    if (indices_loaded)
        return total_rows;

    if (!total_bytes)
        return 0;

    return {};
}

std::optional<UInt64> StorageStripeLog::totalBytes(ContextPtr) const
{
    return total_bytes;
}


void StorageStripeLog::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto lock_timeout = getLockTimeout(backup_entries_collector.getContext());

    loadIndices(lock_timeout);

    ReadLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    if (!file_checker.getFileSize(data_file_path))
        return;

    fs::path data_path_in_backup_fs = data_path_in_backup;
    auto temp_dir_owner = std::make_shared<TemporaryFileOnDisk>(disk, "tmp/");
    fs::path temp_dir = temp_dir_owner->getRelativePath();
    disk->createDirectories(temp_dir);

    const auto & read_settings = backup_entries_collector.getReadSettings();
    const auto & backup_settings = backup_entries_collector.getBackupSettings();
    bool copy_encrypted = !backup_settings.decrypt_files_from_encrypted_disks;
    bool allow_checksums_from_remote_paths = backup_settings.allow_checksums_from_remote_paths;

    /// data.bin
    {
        /// We make a copy of the data file because it can be changed later in write() or in truncate().
        String data_file_name = fileName(data_file_path);
        String hardlink_file_path = temp_dir / data_file_name;
        disk->createHardLink(data_file_path, hardlink_file_path);
        BackupEntryPtr backup_entry = std::make_unique<BackupEntryFromAppendOnlyFile>(
            disk, hardlink_file_path, copy_encrypted, file_checker.getFileSize(data_file_path), allow_checksums_from_remote_paths);
        backup_entry = wrapBackupEntryWith(std::move(backup_entry), temp_dir_owner);
        backup_entries_collector.addBackupEntry(data_path_in_backup_fs / data_file_name, std::move(backup_entry));
    }

    /// index.mrk
    {
        /// We make a copy of the data file because it can be changed later in write() or in truncate().
        String index_file_name = fileName(index_file_path);
        String hardlink_file_path = temp_dir / index_file_name;
        disk->createHardLink(index_file_path, hardlink_file_path);
        BackupEntryPtr backup_entry = std::make_unique<BackupEntryFromAppendOnlyFile>(
            disk, hardlink_file_path, copy_encrypted, file_checker.getFileSize(index_file_path), allow_checksums_from_remote_paths);
        backup_entry = wrapBackupEntryWith(std::move(backup_entry), temp_dir_owner);
        backup_entries_collector.addBackupEntry(data_path_in_backup_fs / index_file_name, std::move(backup_entry));
    }

    /// sizes.json
    String files_info_path = file_checker.getPath();
    backup_entries_collector.addBackupEntry(
        data_path_in_backup_fs / fileName(files_info_path), std::make_unique<BackupEntryFromSmallFile>(disk, files_info_path, read_settings, copy_encrypted));

    /// columns.txt
    backup_entries_collector.addBackupEntry(
        data_path_in_backup_fs / "columns.txt",
        std::make_unique<BackupEntryFromMemory>(getInMemoryMetadata().getColumns().getAllPhysical().toString()));

    /// count.txt
    size_t num_rows = 0;
    for (const auto & block : indices.blocks)
        num_rows += block.num_rows;
    backup_entries_collector.addBackupEntry(
        data_path_in_backup_fs / "count.txt", std::make_unique<BackupEntryFromMemory>(toString(num_rows)));
}

void StorageStripeLog::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto backup = restorer.getBackup();
    if (!backup->hasFiles(data_path_in_backup))
        return;

    if (!restorer.isNonEmptyTableAllowed() && total_bytes)
        RestorerFromBackup::throwTableIsNotEmpty(getStorageID());

    auto lock_timeout = getLockTimeout(restorer.getContext());
    restorer.addDataRestoreTask(
        [storage = std::static_pointer_cast<StorageStripeLog>(shared_from_this()), backup, data_path_in_backup, lock_timeout]
        { storage->restoreDataImpl(backup, data_path_in_backup, lock_timeout); });
}

void StorageStripeLog::restoreDataImpl(const BackupPtr & backup, const String & data_path_in_backup, std::chrono::seconds lock_timeout)
{
    WriteLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    /// Load the indices if not loaded yet. We have to do that now because we're going to update these indices.
    loadIndices(lock);

    /// If there were no files, save zero file sizes to be able to rollback in case of error.
    saveFileSizes(lock);

    try
    {
        fs::path data_path_in_backup_fs = data_path_in_backup;

        /// Append the data file.
        auto old_data_size = file_checker.getFileSize(data_file_path);
        {
            String file_path_in_backup = data_path_in_backup_fs / fileName(data_file_path);
            if (!backup->fileExists(file_path_in_backup))
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File {} in backup is required to restore table", file_path_in_backup);

            backup->copyFileToDisk(file_path_in_backup, disk, data_file_path, WriteMode::Append);
        }

        /// Append the index.
        {
            String index_path_in_backup = data_path_in_backup_fs / fileName(index_file_path);
            IndexForNativeFormat extra_indices;
            if (!backup->fileExists(index_path_in_backup))
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File {} in backup is required to restore table", index_path_in_backup);

            auto index_in = backup->readFile(index_path_in_backup);
            CompressedReadBuffer index_compressed_in{*index_in};
            extra_indices.read(index_compressed_in);

            /// Adjust the offsets.
            for (auto & block : extra_indices.blocks)
            {
                for (auto & column : block.columns)
                    column.location.offset_in_compressed_file += old_data_size;
            }

            insertAtEnd(indices.blocks, std::move(extra_indices.blocks));
        }

        /// Finish writing.
        saveIndices(lock);
        saveFileSizes(lock);
        updateTotalRows(lock);
    }
    catch (...)
    {
        /// Rollback partial writes.
        file_checker.repair();
        removeUnsavedIndices(lock);
        throw;
    }
}


void registerStorageStripeLog(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .has_builtin_setting_fn = StorageLogSettings::hasBuiltin,
    };

    factory.registerStorage("StripeLog", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        String disk_name = getDiskName(*args.storage_def, args.getContext());
        DiskPtr disk = args.getContext()->getDisk(disk_name);

        return std::make_shared<StorageStripeLog>(
            disk,
            args.relative_data_path,
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.mode,
            args.getContext());
    }, features);
}

}
