namespace go kv

enum ErrorCode {
    Good = 0,
    NotFound = -1,
    Unknown = -2 ,
    DataExisted = -3,
    IterExceed = -4
}


struct MapItem {
    1: string key,
    2: string value,
}

struct DataResult {
    1: ErrorCode errorCode
    2: optional MapItem data
}

struct ListData {
    1: ErrorCode errorCode
    2: optional list<MapItem> data
    3: optional list<string> missingkeys
}


service DataService {
    DataResult getData(1:string key),
}

service TDataService{
    DataResult getData(1:string key), 
    ErrorCode putData(1:string key, 2: MapItem data),
    ListData getListData(1:list<string> lskeys),
    ErrorCode removeData(1:string key),
    ErrorCode putMultiData(1:list<MapItem> listData)
}

service StorageService extends TDataService{
    
}