#include <Storages/MergeTree/BernoulliGranuleFilter.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

#include <base/defines.h>

#include <cmath>


namespace DB
{

namespace
{
/// Geometric skip: compute the number of rows to skip before the next hit.
size_t nextGeometricSkip(pcg64 & rng, double log_one_minus_p)
{
    /// Generate a uniform random value u in the open interval (0, 1).
    /// We use the top 53 bits of the 64-bit PCG output (>> 11) to get 53 bits
    /// of mantissa precision (matching IEEE 754 double). Adding 1.0 to both
    /// numerator and denominator ensures u is strictly in (0, 1), avoiding
    /// log(0) in the geometric inverse-CDF below.
    double u = (static_cast<double>(rng() >> 11) + 1.0) * (1.0 / (double(1ULL << 53) + 1.0));
    double raw = std::floor(std::log(u) / log_one_minus_p);
    return raw < double(SIZE_MAX >> 1) ? static_cast<size_t>(raw) : SIZE_MAX >> 1;
}

/// Replay the geometric skip from a checkpoint, calling `callback(offset)` for each hit
/// in [starting_row, starting_row + num_rows). `offset` is relative to starting_row.
void replayRange(
    const std::vector<BernoulliGranuleFilter::Checkpoint> & checkpoints,
    double log_one_minus_p,
    size_t starting_row,
    size_t num_rows,
    auto && callback)
{
    if (checkpoints.empty() || num_rows == 0)
        return;

    /// Binary search for the checkpoint at or before starting_row.
    auto it = std::lower_bound(
        std::begin(checkpoints), std::end(checkpoints), starting_row,
        [](const auto & cp, size_t row) { return cp.mark_start_row < row; });
    size_t lo = (it == std::begin(checkpoints)) ? 0 : std::distance(std::begin(checkpoints), std::prev(it));

    /// Restore RNG state from checkpoint.
    pcg64 rng = checkpoints[lo].rng;
    size_t abs_pos = checkpoints[lo].mark_start_row + checkpoints[lo].remaining_skip;

    size_t end_row = starting_row + num_rows;

    /// Advance past hits before our range.
    while (abs_pos < starting_row)
    {
        size_t skip = nextGeometricSkip(rng, log_one_minus_p) + 1;
        abs_pos += skip;
    }

    /// Emit hits within our range.
    while (abs_pos < end_row)
    {
        callback(abs_pos - starting_row);
        size_t skip = nextGeometricSkip(rng, log_one_minus_p) + 1;
        abs_pos += skip;
    }
}
}


bool BernoulliGranuleFilter::canSkipMark(size_t mark) const
{
    return mark < granules_selected.size() && !granules_selected.test(mark);
}

std::shared_ptr<BernoulliGranuleFilter>
BernoulliGranuleFilter::build(const MergeTreeIndexGranularity & index_granularity, size_t total_rows, Float64 probability, UInt64 part_seed)
{
    auto filter = std::make_shared<BernoulliGranuleFilter>();

    size_t num_marks = index_granularity.getMarksCountWithoutFinal();
    if (num_marks == 0 || total_rows == 0)
        return filter;

    chassert(probability > 0.0 && probability < 1.0);

    filter->log_one_minus_p = std::log(1.0 - probability);
    filter->granules_selected.resize(num_marks, false);
    filter->checkpoints.reserve(num_marks);

    pcg64 rng(part_seed);
    size_t remaining_skip = nextGeometricSkip(rng, filter->log_one_minus_p);

    size_t cumulative_row = 0;
    for (size_t mark = 0; mark < num_marks; ++mark)
    {
        /// Save checkpoint at the start of this mark.
        filter->checkpoints.emplace_back(cumulative_row, remaining_skip, rng);

        size_t rows_in_mark = index_granularity.getMarkRows(mark);
        /// Clamp the last granule to actual row count.
        if (cumulative_row + rows_in_mark > total_rows)
            rows_in_mark = total_rows - cumulative_row;

        /// Walk geometric skip through this mark's rows.
        while (remaining_skip < rows_in_mark)
        {
            filter->granules_selected.set(mark);
            size_t skip = nextGeometricSkip(rng, filter->log_one_minus_p) + 1;
            rows_in_mark -= remaining_skip + 1;
            remaining_skip = skip - 1;
        }

        /// No more hits in this mark — adjust skip counter for next mark.
        remaining_skip -= rows_in_mark;
        cumulative_row += index_granularity.getMarkRows(mark);
        if (cumulative_row > total_rows)
            cumulative_row = total_rows;
    }

    return filter;
}

void BernoulliGranuleFilter::appendToFilter(PaddedPODArray<UInt8> & filter_data, size_t starting_row, size_t num_rows) const
{
    size_t old_size = filter_data.size();
    filter_data.resize_fill(old_size + num_rows, 0);

    replayRange(checkpoints, log_one_minus_p, starting_row, num_rows, [&](size_t offset) { filter_data[old_size + offset] = 1; });
}

void BernoulliGranuleFilter::andWithFilter(
    PaddedPODArray<UInt8> & filter_data, size_t filter_offset, size_t starting_row, size_t num_rows) const
{
    /// First, collect hit positions into a bitset (represented by the existing filter values).
    /// We need to zero all positions that are NOT hits, while preserving the AND with existing values.
    /// Strategy: build a temporary hit bitmap, then AND it with the existing filter.

    PaddedPODArray<UInt8> bernoulli_bits(num_rows, 0);

    replayRange(checkpoints, log_one_minus_p, starting_row, num_rows, [&](size_t offset) { bernoulli_bits[offset] = 1; });

    for (size_t i = 0; i < num_rows; ++i)
        filter_data[filter_offset + i] &= bernoulli_bits[i];
}

}
