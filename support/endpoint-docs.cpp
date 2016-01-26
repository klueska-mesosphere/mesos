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

#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <vector>

#include <mesos/authorizer/authorizer.hpp>

#include <mesos/master/allocator.hpp>

#include <process/help.hpp>
#include <process/limiter.hpp>
#include <process/pid.hpp>
#include <process/sequence.hpp>

#include "master/contender.hpp"
#include "master/detector.hpp"
#include "master/master.hpp"
#include "master/registrar.hpp"
#include "master/repairer.hpp"

#include "slave/gc.hpp"
#include "slave/slave.hpp"
#include "slave/status_update_manager.hpp"

#include "state/in_memory.hpp"
#include "state/storage.hpp"

#include "stout/option.hpp"
#include "stout/os.hpp"
#include "stout/path.hpp"

#include "version/version.hpp"

using namespace mesos::internal;

using mesos::Authorizer;

using mesos::internal::MasterContender;

using mesos::internal::master::Master;
using mesos::internal::master::Registrar;
using mesos::internal::master::Repairer;

using mesos::internal::slave::Containerizer;
using mesos::internal::slave::Fetcher;
using mesos::internal::slave::GarbageCollector;
using mesos::internal::slave::Slave;
using mesos::internal::slave::StatusUpdateManager;

using mesos::master::allocator::Allocator;

using mesos::slave::ResourceEstimator;
using mesos::slave::QoSController;

using process::Future;
using process::Help;
using process::PID;
using process::RateLimiter;
using process::Sequence;
using process::UPID;

using std::cerr;
using std::cout;
using std::endl;
using std::shared_ptr;


// Class to handle extraction of the endpoint help strings from the from the
// global 'help' process
class EndpointHelp
{
public:
  typedef std::map<std::string, std::map<std::string, std::string>> Map;

  Future<const Map> get(const std::vector<UPID>& processes)
  {
    // Sequence used to force a chain of dispatched functions to processes.
    Sequence* s = new Sequence();

// Use collect instead of sequence
    // Force a dispatched function to run on all processes. This guarantees that
    // all processes have been initialized and are up and running. The
    // assumption is that all help strings will have been installed by this
    // point, so we can begin querying the help process to get at them.
    lambda::function<Future<Nothing>(void)> f1;
    for (int i = 0; i < processes.size(); i++) {
      f1 = defer(processes[i], []() { return Nothing(); });
      s->add(f1);
    }

    // Dispatch a get() call on the global process::help object to get at the
    // installed help strings. This is added to the sequence object to
    // guarantee it runs after all other processes have been initialized.
    lambda::function<Future<Map>(void)> f2;
    f2 = defer(process::help, &Help::get);
    Future<Map> helps = s->add(f2);

    // Once the srings are ready, delete the sequence object and return the
    // help strings in a future.
    return helps.then(
      [this, s](const Map helps) -> Future<const Map> {
        delete s;
        return helps;
      });
  }

  // Dump the help strings out to files in a directory.
  Try<Nothing> dumpToFiles(const Map* helps, const std::string& dirname)
  {
    Try<Nothing> e = os::rmdir(dirname, true);
    if (e.isError()) {
      return e;
    }

    e = generateIndex(helps, dirname);
    if (e.isError()) {
      return e;
    }

    for (auto i = helps->begin(); i != helps->end(); i++) {
      for (auto j = i->second.begin(); j != i->second.end(); j++) {
        e = generateHelp(i->first, j->first, j->second, dirname);
        if (e.isError()) {
          return e;
        }
      }
    }

    return Nothing();
  }

private:
  // A standad header on top of every generated .md file.
  std::string getHeader()
  {
    std::string header =
      "<!--- This is an automatically generated file. Do not edit! --->\n\n";
    return header;
  }

  std::string normalizeId(const std::string& id)
  {
    // Some 'id's come in the form <id>([0-9]+). Strip the ([0-9]+) from them.
    // TODO (klueska): Unfortunately, regex is broken in g++ < 4.9, so we can't
    // use regex's until we bump our gcc dependence up to 4.9.
    // std::regex reg("\\([0-9]+\\)");
    // return regex_replace(id, reg, "");
    return id.substr(0, id.find_last_of("("));
  }

  std::string relativeMdPath(
    const std::string& id,
    const std::string& name)
  {
    // Relative .md paths are of the form endpoint/<id>/<name>.md
    // Special case for when name == '/'.
    std::string path;
    std::string new_id = normalizeId(id);
    if (name == "/") {
      path = new_id + ".md";
    } else {
      path = path::join(new_id, name + ".md");
    }
    return path;
  }

  std::string localMdPath(
    const std::string& id,
    const std::string& name,
    const std::string& dirname)
  {
    return path::join(dirname, relativeMdPath(id, name));
  }

  std::string endpointPath(
    const std::string& id,
    const std::string& name)
  {
    return path::join("/", normalizeId(id), name);
  }

  // Generate the index file for all endpoints.
  Try<Nothing> generateIndex(const Map* helps, const std::string& dirname)
  {
    std::string path = path::join(dirname, "index.md");

    std::string output =
      "# HTTP Endpoints #\n"
      "Below is a list of HTTP endpoints available for a given\n"
      "Mesos process.\n"
      "Depending on your configuration, some subset of these endpoints will\n"
      "be available on your Mesos master or agent.\n"
      "Additionally, a '/help' endpoint will be available that displays help\n"
      "for the actual endpoints installed on the running executable.\n\n";

    for (auto s1 = helps->begin(); s1 != helps->end(); s1++) {
      output = output + "##" + normalizeId(s1->first) + "##\n";
      for (auto s2 = s1->second.begin(); s2 != s1->second.end(); s2++) {
        output = output + "* [" + endpointPath(s1->first, s2->first) + "]";
        output = output + "(" + relativeMdPath(s1->first, s2->first) + ")";
        output = output + "\n";
      }
      output += "\n";
    }

    return writeFile(path, output);
  }

  // Dump each individual file
  Try<Nothing> generateHelp(
    const std::string& id,
    const std::string& name,
    const std::string& help,
    const std::string& dirname)
  {
    return writeFile(localMdPath(id, name, dirname), help);
  }

  Try<Nothing> writeFile(const std::string& path, const std::string& input)
  {
    Try<Nothing> e = os::mkdir(Path(path).dirname(), true);
    if (e.isError()) {
      return e;
    }

    Try<int> eint = os::open(
      path,
      O_RDWR | O_TRUNC | O_CREAT,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (eint.isError()) {
      return ErrnoError(eint.error());
    }

    int fd = eint.get();

    // Build the output from the header and the input string
    std::string output = getHeader() + input;

    // Remove all '\n's at the end if there are any and print the output.
    // We know that the string is never empty because we manually insert a
    // non-zero header at the top.
    std::string::size_type pos = output.find_last_not_of('\n');
    output.erase(pos + 1);
    e = os::write(fd, output);

    os::close(fd);
    return e;
  }
};


// Class to set up a minimalistic master process
class MasterProcess
{
public:
  MasterProcess()
  {
    master::Flags flags;

    Option<Authorizer*> authorizer = None();
    Option<shared_ptr<RateLimiter>> slaveRemovalLimiter = None();

    storage = new state::InMemoryStorage();
    state = new state::protobuf::State(storage);
    allocator = Allocator::create(flags.allocator).get();
    registrar = new Registrar(flags, state);
    repairer = new Repairer();
    files = new Files();
    contender = MasterContender::create(None()).get();
    detector = MasterDetector::create(None()).get();

    master = new master::Master(
      allocator,
      registrar,
      repairer,
      files,
      contender,
      detector,
      authorizer,
      slaveRemovalLimiter,
      flags);
  }

  ~MasterProcess()
  {
    delete storage;
    delete state;
    delete allocator;
    delete registrar;
    delete repairer;
    delete files;
    delete contender;
    delete detector;
    delete master;
  }

  operator Master&() const { return *master; }
  operator UPID() const { return master->self(); }

private:
  state::Storage* storage;
  state::protobuf::State* state;
  Allocator* allocator;
  Registrar* registrar;
  Repairer* repairer;
  Files* files;
  MasterContender *contender;
  MasterDetector *detector;
  master::Master *master;
};


// Class to set up a minimalistic slave process
class SlaveProcess
{
public:
  SlaveProcess(std::string master)
  {
    slave::Flags flags;

    fetcher = new Fetcher();
    containerizer = Containerizer::create(flags, false, fetcher).get();
    detector = MasterDetector::create(master).get();
    files = new Files();
    gc = new GarbageCollector();
    statusUpdateManager = new StatusUpdateManager(flags);
    resrcEstimator = ResourceEstimator::create(flags.resource_estimator).get();
    qosController = QoSController::create(flags.qos_controller).get();

    slave = new Slave(
      flags,
      detector,
      containerizer,
      files,
      gc,
      statusUpdateManager,
      resrcEstimator,
      qosController);
  }

  ~SlaveProcess()
  {
    delete fetcher;
    delete containerizer;
    delete detector;
    delete files;
    delete gc;
    delete resrcEstimator;
    delete qosController;
    delete slave;
  }

  operator Slave&() const { return *slave; }
  operator UPID() const { return slave->self(); }

private:
  Fetcher* fetcher;
  Containerizer* containerizer;
  MasterDetector* detector;
  Files* files;
  GarbageCollector* gc;
  StatusUpdateManager* statusUpdateManager;
  ResourceEstimator* resrcEstimator;
  QoSController* qosController;
  Slave *slave;
};


// Class to represent command line flags for the app
class AppFlags : public logging::Flags
{
public:
  AppFlags()
  {
    add(&AppFlags::dirname,
      "dirname",
      "The output directory for the documentation files",
      "",
      [](std::string d) -> Option<Error> {
        if (d == "") {
          return Error("You must supply a directory name for output.");
        }
        return None();
      });
  }

  bool validate(int argc, char **argv)
  {
    Try<Nothing> l = load("", argc, argv);
    if (l.isError()) {
      cerr << usage(l.error()) << endl;
      return false;
    }

    return true;
  }

  bool exitEarly()
  {
    if (help) {
      cout << usage() << endl;
      return true;
    }
    return false;
  }

  std::string dirname;
};


// The main function to extract all endpoint help strings and dump them to files.
int main(int argc, char** argv)
{
  AppFlags flags;

  if (!flags.validate(argc, argv)) {
    return EXIT_FAILURE;
  }
  if (flags.exitEarly()) {
    return EXIT_SUCCESS;
  }

  logging::initialize(argv[0], flags, true);

  VersionProcess version;

  MasterProcess masterProcess;
  Master& master = (Master&)masterProcess;

  SlaveProcess slaveProcess(std::string(master.self()).substr(7));
  Slave& slave = (Slave&)slaveProcess;

  spawn(version);
  spawn(master);
  spawn(slave);

  EndpointHelp eh;
  Future<const EndpointHelp::Map> helps = eh.get({version, master, slave});

// Use defer without an actor to run this.
  helps.then(
    [&eh, &version, &master, &slave, &flags] (const EndpointHelp::Map helps) {
      Try<Nothing> t = eh.dumpToFiles(&helps, flags.dirname);
      if (t.isError()) {
        cerr << t.error() << endl;
      }

      terminate(slave);
      terminate(master);
      terminate(version);

      return t;
    });

  wait(slave);
  wait(master);
  wait(version);

  return EXIT_SUCCESS;
}
