--IMPORT GLOBALS SECTION
require('DBI');
require('logging.file');

--MODULE DEFINE SECTION
luaDBI_MySQL = {}

--OBJECT CREATION FUNCTION
luaDBI_MySQL.new = function(t_dbconn_parameters)
    local self = {}
    
    -- Фикс для luarocks и всего остального
    _G.package.cpath = _G.package.cpath .. ";/usr/lib64/lua/5.1/?.so;/usr/lib/lua/5.1/?.so";
    
    self.logger = logging.file("/var/log/lua/luaDBI_MySQL.log");
    
    local conn, err = DBI.Connect("MySQL",
                                    t_dbconn_parameters["database"],
                                    t_dbconn_parameters["username"],
                                    t_dbconn_parameters["password"],
                                    t_dbconn_parameters["host"],
                                    t_dbconn_parameters["port"]);
        
    if not conn then
        self.logger:error("Error connect to DB:" .. err);
        return nil;
    else
        self.dbconnect = conn;
        self.logger:info("Connection created");
    end;
    
    --API FUNCTION
    self.select = function(s_request)
        if not string.find(s_request, "SELECT") then
            self.logger:error("Not SELECT request:" .. s_request);
            return {"It's not SELECT request",};
        end;
    
        local stmt = assert(self.dbconnect:prepare(s_request));
        local success, err = stmt:execute();
        
        if not success then
            self.logger:error(s_request .. ":" .. err)
            return nil;
        end;

        return stmt:fetch();
    end;
    
    --API FUNCTION
    self.update = function(s_request)
        if not string.find(s_request, "UPDATE") then
            self.logger:error("Not UPDATE request:" .. s_request);
            return {"It's not UPDATE request",};
        end;
    
        local stmt = assert(self.dbconnect:prepare(s_request));
        self.dbconnect:autocommit(true);
        local success, err = stmt:execute();
        self.dbconnect:autocommit(false);
        if not success then
            self.logger:error(s_request .. ":" .. err)
            return false;
        else
            return true;
        end;
    end;
    
    return self;
end;
