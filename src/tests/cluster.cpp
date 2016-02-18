// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <memory>
#include <string>

#include <mesos/mesos.hpp>

#include <mesos/authorizer/authorizer.hpp>

#include <mesos/master/allocator.hpp>

#include <mesos/slave/resource_estimator.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/id.hpp>
#include <process/limiter.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>

#include <stout/duration.hpp>
#include <stout/error.hpp>
#include <stout/gtest.hpp>
#include <stout/none.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#ifdef __linux__
#include "linux/cgroups.hpp"
#endif

#include "authorizer/local/authorizer.hpp"

#include "files/files.hpp"

#include "log/log.hpp"

#include "master/constants.hpp"
#include "master/contender.hpp"
#include "master/detector.hpp"
#include "master/flags.hpp"
#include "master/master.hpp"
#include "master/registrar.hpp"
#include "master/repairer.hpp"

#include "master/allocator/mesos/hierarchical.hpp"

#include "slave/flags.hpp"
#include "slave/gc.hpp"
#include "slave/slave.hpp"
#include "slave/status_update_manager.hpp"

#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/fetcher.hpp"

#include "state/in_memory.hpp"
#include "state/log.hpp"
#include "state/protobuf.hpp"
#include "state/storage.hpp"

#include "zookeeper/url.hpp"

#include "tests/cluster.hpp"


namespace mesos {
namespace internal {
namespace tests {
namespace cluster {

process::Owned<Master> Master::start(
    const master::Flags& flags,
    const Option<zookeeper::URL>& zookeeperUrl,
    const Option<mesos::master::allocator::Allocator*>& allocator,
    const Option<Authorizer*>& authorizer,
    const Option<std::shared_ptr<process::RateLimiter>>& slaveRemovalLimiter)
{
  process::Owned<Master> master(new Master());
  master->zookeeperUrl = zookeeperUrl;

  // NOTE: This lambda closure is necessary in order to use `ASSERT_*`,
  // as these macros require a void return type.
  [=]() {
    // If the allocator is not provided, create a default one.
    if (allocator.isNone()) {
      Try<mesos::master::allocator::Allocator*> _allocator =
        master::allocator::HierarchicalDRFAllocator::create();

      ASSERT_SOME(_allocator);
      master->allocator.reset(_allocator.get());
    }

    // If the authorizer is not provided, create a default one.
    if (authorizer.isNone()) {
      Try<Authorizer*> local = Authorizer::create(master::DEFAULT_AUTHORIZER);
      ASSERT_SOME(local);

      Try<Nothing> initialized = local.get()->initialize(flags.acls.get());
      ASSERT_SOME(initialized);

      master->authorizer.reset(local.get());
    }

    // Create the appropriate master contender/detector.
    if (zookeeperUrl.isSome()) {
      master->contender.reset(new ZooKeeperMasterContender(zookeeperUrl.get()));
      master->masterDetector.reset(
          new ZooKeeperMasterDetector(zookeeperUrl.get()));
    } else {
      master->contender.reset(new StandaloneMasterContender());
      master->masterDetector.reset(new StandaloneMasterDetector());
    }

    ASSERT_FALSE(flags.registry == "in_memory" && flags.registry_strict);
    ASSERT_FALSE(flags.registry == "replicated_log" && flags.work_dir.isNone());
    ASSERT_FALSE(
        flags.registry == "replicated_log" &&
        zookeeperUrl.isSome() &&
        flags.quorum.isNone());

    // Create the replicated-log-based registry, if specified in the flags.
    if (flags.registry == "replicated_log") {
      if (zookeeperUrl.isSome()) {
        // Use ZooKeeper-based replicated log.
        master->log.reset(new log::Log(
            flags.quorum.get(),
            path::join(flags.work_dir.get(), "replicated_log"),
            zookeeperUrl.get().servers,
            flags.zk_session_timeout,
            path::join(zookeeperUrl.get().path, "log_replicas"),
            zookeeperUrl.get().authentication,
            flags.log_auto_initialize));
      } else {
        master->log.reset(new log::Log(
            1,
            path::join(flags.work_dir.get(), "replicated_log"),
            std::set<process::UPID>(),
            flags.log_auto_initialize));
      }
    }

    // Create the registry's storage backend.
    if (flags.registry == "in_memory") {
      master->storage.reset(new state::InMemoryStorage());
    } else if (flags.registry == "replicated_log") {
      master->storage.reset(new state::LogStorage(master->log.get()));
    } else {
      FAIL() << "'" << flags.registry << "' is not a supported option for"
             << " registry persistence";
    }

    // Instantiate some other master dependencies.
    master->state.reset(new state::protobuf::State(master->storage.get()));
    master->registrar.reset(new master::Registrar(flags, master->state.get()));
    master->repairer.reset(new master::Repairer());

    if (slaveRemovalLimiter.isNone() &&
        flags.slave_removal_rate_limit.isSome()) {
      // Parse the flag value.
      // TODO(vinod): Move this parsing logic to flags once we have a
      // 'Rate' abstraction in stout.
      std::vector<std::string> tokens =
        strings::tokenize(flags.slave_removal_rate_limit.get(), "/");

      ASSERT_FALSE(tokens.size() != 2);

      Try<int> permits = numify<int>(tokens[0]);
      ASSERT_SOME(permits);

      Try<Duration> duration = Duration::parse(tokens[1]);
      ASSERT_SOME(duration);

      master->slaveRemovalLimiter = std::shared_ptr<process::RateLimiter>(
          new process::RateLimiter(permits.get(), duration.get()));
    }

    // Inject all the dependencies.
    master->master.reset(new master::Master(
        allocator.getOrElse(master->allocator.get()),
        master->registrar.get(),
        master->repairer.get(),
        &master->files,
        master->contender.get(),
        master->masterDetector.get(),
        authorizer.getOrElse(master->authorizer.get()),
        slaveRemovalLimiter.isSome() ?
          slaveRemovalLimiter :
          master->slaveRemovalLimiter,
        flags));

    // If we are using the `StandaloneMasterDetector`, appoint the only
    // master as the leading master.
    if (zookeeperUrl.isNone()) {
      StandaloneMasterDetector* _detector =
        CHECK_NOTNULL(dynamic_cast<StandaloneMasterDetector*>(
            master->masterDetector.get()));

      _detector->appoint(master->master->info());
    }

    process::Future<Nothing> _recover =
      FUTURE_DISPATCH(master->master->self(), &master::Master::_recover);

    master->pid = process::spawn(master->master.get());

    // Speed up the tests by ensuring that the Master is recovered
    // before the test proceeds. Otherwise, authentication and
    // registration messages may be dropped, causing delayed retries.
    // NOTE: We use process::internal::await() to avoid awaiting a
    // Future forever when the Clock is paused.
    if (!process::internal::await(
            _recover,
            flags.registry_fetch_timeout + flags.registry_store_timeout)) {
      FAIL() << "Failed to wait for _recover";
    }

    bool paused = process::Clock::paused();

    // Need to settle the Clock to ensure that the Master finishes
    // executing _recover() before we return.
    process::Clock::pause();
    process::Clock::settle();

    // Return the Clock to its original state.
    if (!paused) {
      process::Clock::resume();
    }
  }();

  return master;
}


Master::~Master()
{
  // NOTE: Authenticators' lifetimes are tied to libprocess's lifetime.
  // This means that multiple masters in tests are not supported.
  process::http::authentication::unsetAuthenticator(
      master::DEFAULT_HTTP_AUTHENTICATION_REALM);

  process::terminate(pid);
  process::wait(pid);
}


process::Owned<MasterDetector> Master::detector()
{
  if (zookeeperUrl.isSome()) {
    return process::Owned<MasterDetector>(
        new ZooKeeperMasterDetector(zookeeperUrl.get()));
  }

  return process::Owned<MasterDetector>(new StandaloneMasterDetector(pid));
}


MasterInfo Master::getMasterInfo()
{
  return master->info();
}


process::Owned<Slave> Slave::start(
    MasterDetector* detector,
    const slave::Flags& flags,
    const Option<std::string>& id,
    const Option<slave::Containerizer*>& containerizer,
    const Option<slave::GarbageCollector*>& gc,
    const Option<slave::StatusUpdateManager*>& statusUpdateManager,
    const Option<mesos::slave::ResourceEstimator*>& resourceEstimator,
    const Option<mesos::slave::QoSController*>& qosController)
{
  process::Owned<Slave> slave(new Slave());

  // NOTE: This lambda closure is necessary in order to use `ASSERT_*`,
  // as these macros require a void return type.
  [=]() {
    // TODO(benh): Create a work directory if using the default.

    slave->flags = flags;
    slave->detector = detector;

    // If the containerizer is not provided, create a default one.
    if (containerizer.isSome()) {
      slave->containerizer = containerizer.get();
    } else {
      // Create a new fetcher.
      slave->fetcher.reset(new slave::Fetcher());

      Try<slave::Containerizer*> _containerizer =
        slave::Containerizer::create(flags, true, slave->fetcher.get());

      ASSERT_SOME(_containerizer);
      slave->ownedContainerizer.reset(_containerizer.get());
      slave->containerizer = _containerizer.get();
    }

    // If the garbage collector is not provided, create a default one.
    if (gc.isNone()) {
      slave->gc.reset(new slave::GarbageCollector());
    }

    // If the resource estimator is not provided, create a default one.
    if (resourceEstimator.isNone()) {
      Try<mesos::slave::ResourceEstimator*> _resourceEstimator =
        mesos::slave::ResourceEstimator::create(flags.resource_estimator);

      ASSERT_SOME(_resourceEstimator);
      slave->resourceEstimator.reset(_resourceEstimator.get());
    }

    // If the QoS controller is not provided, create a default one.
    if (qosController.isNone()) {
      Try<mesos::slave::QoSController*> _qosController =
        mesos::slave::QoSController::create(flags.qos_controller);

      ASSERT_SOME(_qosController);
      slave->qosController.reset(_qosController.get());
    }

    // If the status update manager is not provided, create a default one.
    if (statusUpdateManager.isNone()) {
      slave->statusUpdateManager.reset(new slave::StatusUpdateManager(flags));
    }

    // Inject all the dependencies.
    slave->slave.reset(new slave::Slave(
        id.isSome() ? id.get() : process::ID::generate("slave"),
        flags,
        detector,
        slave->containerizer,
        &slave->files,
        gc.getOrElse(slave->gc.get()),
        statusUpdateManager.getOrElse(slave->statusUpdateManager.get()),
        resourceEstimator.getOrElse(slave->resourceEstimator.get()),
        qosController.getOrElse(slave->qosController.get())));

    slave->pid = process::spawn(slave->slave.get());
  }();

  return slave;
}


Slave::~Slave()
{
  // NOTE: There are some tests that only want to emulate the slave's
  // shutdown logic without the associated cleanup.
  if (isShutdown) {
    return;
  }

  // NOTE: This lambda closure is necessary in order to use `ASSERT_*`,
  // as these macros require a void return type.
  [=]() {
    // Destroy the existing containers on the slave. Note that some
    // containers may terminate while we are doing this, so we ignore
    // any 'wait' failures and ensure that there are no containers
    // when we're done destroying.
    process::Future<hashset<ContainerID>> containers =
      containerizer->containers();

    AWAIT_READY(containers);

    foreach (const ContainerID& containerId, containers.get()) {
      process::Future<containerizer::Termination> wait =
        containerizer->wait(containerId);

      containerizer->destroy(containerId);

      AWAIT(wait);
    }

    containers = containerizer->containers();
    AWAIT_READY(containers);

    ASSERT_TRUE(containers.get().empty())
      << "Failed to destroy containers: " << stringify(containers.get());
  }();

  terminate();
}


void Slave::shutdown()
{
  // NOTE: There are some tests that only want to emulate the slave's
  // shutdown logic without the associated cleanup.
  isShutdown = true;

  process::dispatch(slave.get(), &slave::Slave::shutdown, process::UPID(), "");
  wait();
}


void Slave::terminate()
{
  // NOTE: There are some tests that only want to emulate the slave's
  // shutdown logic without the associated cleanup.
  isShutdown = true;

  process::terminate(pid);
  wait();
}


void Slave::wait()
{
  process::wait(pid);

#ifdef __linux__
  // Remove all of this processes threads into the root cgroups - this
  // simulates the slave process terminating and permits a new slave to start
  // when the --slave_subsystems flag is used.
  if (flags.slave_subsystems.isSome()) {
    foreach (const std::string& subsystem,
             strings::tokenize(flags.slave_subsystems.get(), ",")) {
      std::string hierarchy = path::join(flags.cgroups_hierarchy, subsystem);

      std::string cgroup = path::join(flags.cgroups_root, "slave");

      Try<bool> exists = cgroups::exists(hierarchy, cgroup);
      if (exists.isError() || !exists.get()) {
        EXIT(1) << "Failed to find cgroup " << cgroup
                << " for subsystem " << subsystem
                << " under hierarchy " << hierarchy
                << " for slave: " + exists.error();
      }

      // Move all of our threads into the root cgroup.
      Try<Nothing> assign = cgroups::assign(hierarchy, "", getpid());
      if (assign.isError()) {
        EXIT(1) << "Failed to move slave threads into cgroup " << cgroup
                << " for subsystem " << subsystem
                << " under hierarchy " << hierarchy
                << " for slave: " + assign.error();
      }
    }
  }
#endif // __linux__
}

} // namespace cluster {
} // namespace tests {
} // namespace internal {
} // namespace mesos {
