#MapReduceApi.py

import os
import commands

class MapReduceBox(object):
  log = ""
  hadoop = {}
  params = {}
  options = {}
  custom_flags = {}
  custom_params = {}

  def __init__(self, queue="page_search", email="cs.xuyd@gmail.com"):
    self.hadoop["hdfs_host"] = "hdfs://XXXXXX"
    self.hadoop["hdfs_server"] = "XXXXXX"
    self.hadoop["hadoop_binary"] = "/opt/hadoop/program/bin/hadoop"
    self.params["hdfs_input_paths"] = ""
    self.params["hdfs_output_dir"] = ""
    self.params["num_mapper"] = ""
    self.params["num_reducer"] = ""
    self.params["input_format"] = ""
    self.params["output_format"] = ""
    self.params["hdfs_input_paths"] = ""
    self.options["find_sstable_recursive"] = ""
    self.options["compress_map_output"] = ""
    self.options["compress_mapper_out_value"] = ""
    self.options["enable_multi_mapper_input"] = ""
    self.options["enable_multi_mapper_output"] = ""
    self.options["release_data_prefix"] = "/var/data/static/"
    self.options["uploading_files"] = ""
    self.custom_params["mapred.job.queue.name"] = queue
    self.custom_params["user.email"] = email
    self.custom_params["mapred.job.map.memory.mb"] = ""
    self.custom_params["mapred.job.reduce.memory.mb"] = ""

  def getCommand(self):
    cmd = self.binary
    for (key, val) in self.hadoop.items():
      cmd = cmd + self.__createParam(key, val)
    for (key, val) in self.params.items():
      cmd = cmd + self.__createParamAndCheck(key, val)
    for (key, val) in self.options.items():
      cmd = cmd + self.__createOption(key, val)
    for (key, val) in self.custom_flags.items():
      cmd = cmd + self.__createParamAndCheck(key, val)
    cplist = [key+":"+val for (key,val) in self.custom_params.items() if val]
    cmd = cmd + " --custom_mapred_params=" + "^o^".join(cplist)
    return cmd

  def printCommand(self):
    cmd = self.getCommand()
    cmd = cmd.replace(" ", "\n")
    print cmd

  def setBinary(self, name):
    if name[0] != "/":
      pwd = commands.getoutput("pwd")
      name = pwd + "/" + name
    if not os.path.exists(name):
      assert(0), "Map-Reduce binary doesn't exists"
    self.binary = name

  def setMapRedNum(self, mn, rn):
    self.params["num_mapper"] = str(mn)
    self.params["num_reducer"] = str(rn)
    if not rn:
      self.options["compress_map_output"] = "false"
      self.options["compress_mapper_out_value"] = "false"

  def setIOFormat(self, in_format, out_format, recursive=True):
    self.params["input_format"] = in_format
    self.params["output_format"] = out_format
    if not cmp(in_format, "sstable"):
      self.options["find_sstable_recursive"] = "true" if recursive else "false"
    if not cmp(out_format, "multi_sstable"):
      self.options["multi_sstable_reducer_output_dirs"] = "true"

  def setInputPath(self, path):
    print "set hdfs input path ", path
    files = path.split(",")
    for f in files:
      cmd = "hadoop fs -ls " + f
      stat,out = commands.getstatusoutput(cmd)
      if stat:
        print out
        assert(not stat), "check hdfs input path failed"
    self.params["hdfs_input_paths"] = path

  def setOutputPath(self, path, rep=True):
    print "set hdfs output dir ", path
    cmd = "hadoop fs -ls " + path
    stat,out = commands.getstatusoutput(cmd)
    if stat:
      cmd = "hadoop fs -mkdir " + path
      print cmd
      result = os.system(cmd)
      assert(not result), "create hdfs output dir failed"
    elif not rep:
      print out.split("\n")[0]
      print "Warning: Are you sure to replace " + path + " ?(yes,no)"
      answer = raw_input("> ")
      assert(cmp(answer, "no")), "create hdfs output dir dropped"
    else:
      pass
    self.params["hdfs_output_dir"] = path

  def setHdfsBinDir(self, path):
    self.params["hdfs_bin_dir"] = path

  def setJvmMemory(self, size):
    self.options["jvm_memory_mb_size"] = str(size)

  def setMemoryLimits(self, mapmb, redmb):
    self.custom_params["mapred.job.map.memory.mb"] = str(mapmb)
    self.custom_params["mapred.job.reduce.memory.mb"] = str(redmb)

  def setMultiMapperIO(self, ei, eo):
    if ei:
      self.options["enable_multi_mapper_input"] = "true"
    if eo:
      self.options["enable_multi_mapper_output"] = "true"

  def setUpLoadingFile(self, files):
    self.options["uploading_files"] = files

  def setReleaseYrDataPrefix(self, prefix):
    self.options["release_yrdata_prefix"] = prefix

  def setThriftRPC(self, server, port):
    self.options["hadoop_fs_thrift_server"] = server
    self.options["hadoop_fs_thrift_port"] = str(port)

  def setOutputSubDir(self, output_type, output_name):
    output_flags = output_type + "_output_subdir"
    self.options[output_flags] = output_name

  def setCustomFlags(self, key, val):
    self.custom_flags[key] = val;

  def setLogs(self, log):
    pos = log.rfind("/")
    if not(pos==-1):
      path = log[:pos]
      if not os.path.exists(path):
        print "create log dir: ", path
        os.mkdir(path)
    self.log = log

  def setLocalInput(self, path):
    assert(os.path.exists(path)), "failed to find local input path"
    self.params["hdfs_input_paths"] = path

  def setLocalOutput(self, path):
    if not os.path.exists(path):
      print "create output path: ", path
      os.mkdir(path)
    self.params["hdfs_output_dir"] = path

  def localRun(self, debug = True):
    self.hadoop["local_run"] = "true"
    if debug:
      self.binary = "gdb --args " + self.binary
    self.__execute()

  def hadoopRun(self):
    self.hadoop["auto_run"] = "true"
    self.__execute()

  def __execute(self):
    cmd = self.getCommand()
    if self.log:
      stdout = self.log + ".out"
      stderr = self.log + ".err"
      cmd = cmd + " 1>" + stdout + " 2>" + stderr
      print "Map-Reduce log is redirected to ", self.log
    return os.system(cmd)

  def __createParam(self, key, val):
    return " --" + key + "=" + val

  def __createParamAndCheck(self, key, val):
    if not val:
      print "Error: " + key + " is empty!"
      assert(val), "map-reduce params is emtpy"
    return self.__createParam(key, val)

  def __createOption(self, key, val):
    if not val:
      return ""
    return self.__createParam(key, val)

# global methods
def partName(idx):
  name = str(idx)
  while len(name) < 5:
    name = "0" + name
  return "part-" + name

def putToHdfs(src, dst):
  if type(src) == type("x"):
    src = [src]
  for item in src:
    cmd = "hadoop fs -put " + item + " " + dst
    print cmd
    assert(not os.system(cmd)), "hadoop fs -put failed"

def getToLocal(src, dst):
  if type(src) == type("x"):
    src = [src]
  for item in src:
    cmd = "hadoop fs -get " + item + " " + dst
    print cmd
    assert(not os.system(cmd)), "hadoop fs -get failed"

def mergeToLocal(src, dst):
  cmd = "hadoop fs -getmerge " + src + " " + dst
  print cmd
  assert(not os.system(cmd)), "hadoop fs -getmerge failed"

def moveInHdfs(src, dst):
  if type(src) == type("x"):
    src = [src]
  for item in src:
    cmd = "hadoop fs -mv " + item + " " + dst
    print cmd
    assert(not os.system(cmd)), "hadoop fs -mv failed"

def removeInHdfs(src, sure):
  if type(src) == type("x"):
    src = [src]
  for item in src:
    cmd = "hadoop fs -rmr " + item
    print cmd
    if not sure:
      print "Warning: remove is un-recoverable, are you sure?(yes,no)"
      answer = raw_input("> ")
      if not cmp(answer, "yes"):
        os.system(cmd)
    else:
      os.system(cmd)

if __name__ == '__main__':
  mrbox = MapReduceBox()
  mrbox.setBinary("tf_idf_mr")
  mrbox.setMapRedNum(100, 50)
  mrbox.setIOFormat("sstable", "text")
  mrbox.setInputPath("/home/html/")
  mrbox.setOutputPath("/home/xuyd/output/")
  mrbox.setHdfsBinDir("/home/xuyd/bin/")
  mrbox.printCommand()
  result = mrbox.hadoopRun()
  assert(not result), "Map-Reduce Execution Failed"
  print "Map-Reduce Execution Succeeded"
