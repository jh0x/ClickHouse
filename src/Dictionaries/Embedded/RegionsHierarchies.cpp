#include <Dictionaries/Embedded/RegionsHierarchies.h>

#include <Poco/DirectoryIterator.h>
#include <Common/logger_useful.h>

namespace DB
{

RegionsHierarchies::RegionsHierarchies(IRegionsHierarchiesDataProviderPtr data_provider)
{
    LoggerPtr log = getLogger("RegionsHierarchies");

    LOG_DEBUG(log, "Adding default regions hierarchy");
    data.emplace("", data_provider->getDefaultHierarchySource());

    for (const auto & name : data_provider->listCustomHierarchies())
    {
        LOG_DEBUG(log, "Adding regions hierarchy for {}", name);
        data.emplace(name, data_provider->getHierarchySource(name));
    }

    reload();
}

}
