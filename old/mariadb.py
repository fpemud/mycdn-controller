

class _MariadbServer:

    def __init__(self, param):
        self.param = param
        self._dataDir = os.path.join(McConst.tmpDir, "mariadb.data")
        self._cfgFile = os.path.join(McConst.tmpDir, "mariadb.cnf")
        self._logFile = os.path.join(McConst.logDir, "mariadb.log")
        self._dbRootPassword = "root"

        self._dirDict = dict()
        self._tableInfoDict = dict()

        self._socketFile = None
        self._port = None
        self._proc = None

    @property
    def socketFile(self):
        assert self._proc is not None
        return self._socketFile

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def start(self):
        assert self._proc is None
        self._socketFile = os.path.join(McConst.tmpDir, "mariadb.socket")
        self._initialize()
        self._port = McUtil.getFreeSocketPort("tcp")
        self._start()
        self._initializeAfterStart()
        logging.info("Slave server (mariadb) started, listening on port %d." % (self._port))

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        if self._port is not None:
            self._port = None
        if self._socketFile is not None:
            assert not os.path.exists(self._socketFile)
            self._socketFile = None

    def addDatabaseDir(self, databaseName, tableDir, tableInfo):
        # tableInfo { "table-name": ( block-size, "table-schema" ) }
        assert self._proc is not None
        assert _checkNameAndRealPath(self._dirDict, databaseName, tableDir)
        self._prepareTableDir(tableDir, tableInfo)
        self._dirDict[databaseName] = tableDir
        self._tableInfoDict[databaseName] = tableInfo

    def _initialize(self):
        # mariadb-install-db
        with open(self._logFile, "w") as f:
            f.write("## mariadb-install-db #######################\n")
        McUtil.mkDirAndClear(self._dataDir)
        cmd = "/usr/share/mariadb/scripts/mariadb-install-db %s >>%s" % (" ".join(self.__commonOptions()), self._logFile)
        McUtil.shellCall(cmd)

        # mysql_secure_installation
        with open(self._logFile, "a") as f:
            f.write("\n")
            f.write("## mysql_secure_installation #######################\n")
        proc = None
        child = None
        try:
            proc = sc = subprocess.Popen(["/usr/sbin/mysqld"] + self.__commonOptions())
            while not os.path.exists(self._socketFile):
                time.sleep(1.0)

            with open(self._logFile, "ab") as f:
                child = pexpect.spawn("/usr/bin/mysql_secure_installation --no-defaults --socket=%s" % (self._socketFile), logfile=f)
                child.expect('Enter current password for root \\(enter for none\\): ')
                child.sendline("")
                child.expect("Switch to unix_socket authentication \\[Y/n\\] ")
                child.sendline('n')
                child.expect('Change the root password\\? \\[Y/n\\] ')
                child.sendline('Y')
                child.expect('New password: ')
                child.sendline(self._dbRootPassword)
                child.expect('Re-enter new password: ')
                child.sendline(self._dbRootPassword)
                child.expect('Remove anonymous users\\? \\[Y/n\\] ')
                child.sendline('Y')
                child.expect('Disallow root login remotely\\? \\[Y/n\\] ')
                child.sendline('Y')
                child.expect('Remove test database and access to it\\? \\[Y/n\\] ')
                child.sendline('Y')
                child.expect('Reload privilege tables now\\? \\[Y/n\\] ')
                child.sendline('n')
                child.expect(pexpect.EOF)
        finally:
            if child is not None:
                child.terminate()
                child.wait()
            if proc is not None:
                proc.terminate()
                proc.wait()

    def _start(self):
        # generate mariadb config file
        with open(self._cfgFile, "w") as f:
            buf = ""
            buf += "[mysqld]\n"
            f.write(buf)

        # start mariadb
        self._proc = subprocess.Popen(["/usr/sbin/mysqld"] + self.__commonOptions())
        while not os.path.exists(self._socketFile):
            time.sleep(1.0)

    def _prepareDatabaseDir(self, tableDir, tableInfo):
        newTableNameList = []
        changeTableNameList = []
        errorTableNameList = []

        for tableName, value in tableInfo.items():
            blockSize, tableSchema = value
            ret = self.__getTableFileState(tableDir, tableName, blockSize, tableSchema)
            if ret == "ok":
                pass
            elif ret == "not-exist":
                newTableNameList.append()
            elif ret == "changed":

            elif ret == "error":
            else:
                assert False


            if not os.path.exists(tableSchemaFile):
                newTableNameList.append(tableName)
                continue

            if McUtil.readFile(tableSchemaFile) != tableSchemaFile:
                changeTableNameList.append(tableName)
                continue

            if os.path.exist




        # mariadb-install-db
        with open(self._logFile, "w") as f:
            f.write("## mariadb-install-db #######################\n")
        McUtil.mkDirAndClear(self._dataDir)
        cmd = "/usr/share/mariadb/scripts/mariadb-install-db %s >>%s" % (" ".join(self.__commonOptions()), self._logFile)
        McUtil.shellCall(cmd)

    def __getTableFileState(self, tableDir, tableName, blockSize, tableSchema):
        # returns "ok", "not-exist", "changed", "error"

        tableSchemaFile = os.path.join(tableDir, tableName + ".schema")
        tableIbdFileReal = os.path.join(tableDir, tableName + ".ibd.real")
        tableCfgFileReal = os.path.join(tableDir, tableName + ".cfg.real")
        tableIbdFile = os.path.join(tableDir, tableName + ".ibd")               # mariadb standard
        tableCfgFile = os.path.join(tableDir, tableName + ".cfg")               # mariadb standard

        if not os.path.exists(tableSchemaFile):
            return "not-exist"

        if McUtil.readFile(tableSchemaFile) != tableSchemaFile:
            return "changed"

        if not os.path.exists(tableIbdFileReal):
            return "error"
        if not os.path.exists(tableCfgFileReal):
            return "error"
        if not os.path.exists(tableIbdFile):
            return "error"
        if not os.path.exists(tableCfgFile):
            return "error"
        if not os.path.islink(tableIbdFile) and not os.readlink(tableIbdFile) != tableName + ".ibd":
            return "error"
        if not os.path.islink(tableCfgFile) and not os.readlink(tableCfgFile) != tableName + ".cfg":
            return "error"

        return "ok"






# # mysql_secure_installation
# with open(logFile, "a") as f:
#     f.write("\n")
#     f.write("## mysql_secure_installation #######################\n")
# proc = None
# child = None
# try:
#     proc = subprocess.Popen(["/usr/sbin/mysqld"] + self.__commonOptions())
#     McUtil.waitTcpServiceForProc(self.param.listenIp, self._port, proc)
#     with open(logFile, "ab") as f:
#         child = pexpect.spawn("/usr/bin/mysql_secure_installation --no-defaults --socket=%s" % (self._socketFile), logfile=f)
#         child.expect('Enter current password for root \\(enter for none\\): ')
#         child.sendline("")
#         child.expect("Switch to unix_socket authentication \\[Y/n\\] ")
#         child.sendline('n')
#         child.expect('Change the root password\\? \\[Y/n\\] ')
#         child.sendline('Y')
#         child.expect('New password: ')
#         child.sendline(self._dbRootPassword)
#         child.expect('Re-enter new password: ')
#         child.sendline(self._dbRootPassword)
#         child.expect('Remove anonymous users\\? \\[Y/n\\] ')
#         child.sendline('Y')
#         child.expect('Disallow root login remotely\\? \\[Y/n\\] ')
#         child.sendline('Y')
#         child.expect('Remove test database and access to it\\? \\[Y/n\\] ')
#         child.sendline('Y')
#         child.expect('Reload privilege tables now\\? \\[Y/n\\] ')
#         child.sendline('n')
#         child.expect(pexpect.EOF)
# finally:
#     if child is not None:
#         child.terminate()
#         child.wait()
#     if proc is not None:
#         proc.terminate()
#         proc.wait()
