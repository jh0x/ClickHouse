#include <Disks/DiskFromAST.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>
#include <Common/SipHash.h>
#include <Common/Config/ConfigProcessor.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/DiskSelector.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/isDiskFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <queue>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::string getOrCreateCustomDisk(
    const ASTs & disk_args,
    const std::string & serialization,
    ContextPtr context,
    bool attach)
{
    const auto & server_config = context->getConfigRef();
    std::string include_from_path;
    if (server_config.has("include_from"))
        include_from_path = server_config.getString("include_from");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    {
        auto xml_document = getDiskConfigurationFromASTImpl(disk_args, context);

        Poco::AutoPtr<Poco::XML::NamePool> name_pool(new Poco::XML::NamePool());
        Poco::XML::DOMParser dom_parser(name_pool);

        std::vector<std::pair<std::string, std::string>> substitutions;
        zkutil::ZooKeeperNodeCache zk_node_cache([&]() { return context->getZooKeeper(); });

        ConfigProcessor::processIncludes(
            xml_document,
            substitutions,
            include_from_path,
            /* throw_on_bad_incl */!attach,
            dom_parser,
            getLogger("getOrCreateCustomDisk"),
            {}, {},
            &zk_node_cache);

        config->load(xml_document);
    }

    Poco::Util::AbstractConfiguration::Keys disk_settings_keys;
    config->keys(disk_settings_keys);
    /// Check that no settings are defined when disk from the config is referred.
    if (disk_settings_keys.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk function must have arguments. Invalid disk description.");

    if (disk_settings_keys.size() == 1 && disk_settings_keys.front() == "name" && !attach)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk function `{}` must have other arguments apart from `name`, which describe disk configuration. Invalid disk description.",
            serialization);

    auto disk_settings_hash = sipHash128(serialization.data(), serialization.size());

    std::string disk_name;
    if (config->has("name"))
    {
        disk_name = config->getString("name");
    }
    else
    {
        /// We need a unique name for a created custom disk, but it needs to be the same
        /// after table is reattached or server is restarted, so take a hash of the disk
        /// configuration serialized ast as a disk name suffix.
        disk_name = DiskSelector::TMP_INTERNAL_DISK_PREFIX + toString(disk_settings_hash);
    }

    auto disk = context->getOrCreateDisk(disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
        auto result = DiskFactory::instance().create(
            disk_name, *config, /* config_path */"", context, disks_map, /* attach */attach, /* custom_disk */true);
        /// Mark that disk can be used without storage policy.
        result->markDiskAsCustom(disk_settings_hash);
        return result;
    });

    if (!disk->isCustomDisk())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Disk `{}` already exists and is described by the config."
            " It is impossible to redefine it.",
            disk_name);

    if (disk->getCustomDiskSettings() != disk_settings_hash && !attach)
        throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The disk `{}` is already configured as a custom disk in another table. It can't be redefined with different settings.",
                disk_name);

    if (!attach && !disk->isRemote() && disk->getName() != "backup")
    {
        static constexpr auto custom_local_disks_base_dir_in_config = "custom_local_disks_base_directory";
        auto disk_path_expected_prefix = context->getConfigRef().getString(custom_local_disks_base_dir_in_config, "");

        if (disk_path_expected_prefix.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Base path for custom local disks must be defined in config file by `{}`",
                custom_local_disks_base_dir_in_config);

        if (!pathStartsWith(disk->getPath(), disk_path_expected_prefix))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Path of the custom local disk must be inside `{}` directory",
                disk_path_expected_prefix);
    }

    return disk_name;
}

class DiskConfigurationFlattener
{
public:
    struct Data
    {
        ContextPtr context;
        bool attach;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (isDiskFunction(ast))
        {
            const auto * function = ast->as<ASTFunction>();
            const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function->arguments.get());
            const auto & function_args = function_args_expr->children;
            auto disk_setting_string = function->formatWithSecretsOneLine();
            auto disk_name = getOrCreateCustomDisk(function_args, disk_setting_string, data.context, data.attach);
            ast = std::make_shared<ASTLiteral>(disk_name);
        }
    }
};


std::string DiskFromAST::createCustomDisk(const ASTPtr & disk_function_ast, ContextPtr context, bool attach)
{
    if (!isDiskFunction(disk_function_ast))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a disk function");

    auto ast = disk_function_ast->clone();

    using FlattenDiskConfigurationVisitor = InDepthNodeVisitor<DiskConfigurationFlattener, false>;
    FlattenDiskConfigurationVisitor::Data data{context, attach};
    FlattenDiskConfigurationVisitor{data}.visit(ast);

    return assert_cast<const ASTLiteral &>(*ast).value.safeGet<String>();
}

void DiskFromAST::ensureDiskIsNotCustom(const std::string & disk_name, ContextPtr context)
{
    auto disk = context->getDisk(disk_name);

    if (disk->isCustomDisk())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk name `{}` is a custom disk that is used in other table. "
            "That disk could not be used by a reference by other tables. The custom disk should be fully specified with a disk function.",
            disk_name);
}

}
