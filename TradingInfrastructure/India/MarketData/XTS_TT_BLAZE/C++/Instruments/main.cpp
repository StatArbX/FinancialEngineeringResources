#include "main.h"

// [6]
size_t WriteCallBack(void* contents,size_t size,size_t nmembs, std::string* response) {
    size_t totalSize = size*nmembs;
    response->append(static_cast<char*>(contents),totalSize);
    return totalSize;
}

std::string getCurrentTime(){
    std::time_t t = std::time(nullptr);
    std::tm timeObj = *std::localtime(&t);
    std::ostringstream oss;
    oss<< std::put_time(&timeObj,"%Y-%m%-d");
    return oss.str();
}

std::string getCurrentDatetime(){
    std::time_t t = std::time(nullptr);
    std::tm timeObj = *std::localtime(&t);
    std::ostringstream oss;
    oss<< std::put_time(&timeObj, "%Y-%m%-d %H:%M:%S");
    return oss.str();
    
}

void logEvent(const std::string& message, const int& event){
    std::string current_date = getCurrentTime();
    std::string current_datetime = getCurrentDatetime();
    std::string log_filename = "eventsLogger_" + current_time + ".log";
    std::ofstream log_file(log_filename, std::ios_base::app);

    if(!log_file){
        std::cerr<<"Error Opening Log File."<<std::endl;
    }

    if(log_file.is_open()){
        switch (event){
            case 1:
                log_file<<current_datetime<< " | "<<"Event: "<<message<<std::endl;
                break;    
            case 2:
                log_file<<current_datetime<< " | "<<"Error: "<<message<<std::endl;
                break;   
            case default:
                log_file<<current_datetime<< " | "<<"Unknown Error/Event: "<<message<<std::endl;
                break;
        }
    }
}

struct Bid {
    int Size;
    double Price;
    int TotalOrders;
    int BuyBackMarketMaker;
};

struct Ask {
    int Size;
    double Price;
    int TotalOrders;
    int BuyBackMarketMaker;
};


MarketDataAPIFunctions::MarketDataAPIFunctions() {};


/*
Below is the format for Quotes function json payload:
{
  "instruments": [
    {
      "exchangeSegment": 1,
      "exchangeInstrumentID": 22
    }
  ],
  "xtsMessageCode": 1502,
  "publishFormat": "JSON"
}
*/

std::tuple<std::vector<Bid>,std::vector<Ask>> MarketDataAPIFunctions::Quotes(const std::string& token,
                               const int& exchangeSegment,
                               const int& exchangeInstrumentID,
                               const int& xtsMessageCode,
                               const std::string& publishFormat){
    
    /* 
    Define all the variables to be used here:
    */
    CURL* curl = nullptr;
    std::string response_buffer;
    CURLcode res;
    std::string json_payload_string;
    struct curl_slist* headers = nullptr;
    Json::Value instruments_json;
    Json::Value payload_json;

    // This is for read write operations from json to string to send a payload in string form:
    Json::StreamWriterBuilder writer;

    // This is for reading the response from the server:
    Json::CharReaderBuilder reader;
    Json::Value response_data;
    std::string errors;

    // This is for reading the response from the server for the listQutoes key/value:
    Json::CharReaderBuilder list_quotes_stream_reader;
    Json::Value list_quotes_data;
    std::string list_quotes_errors;

    // These vectors will be initialised here and the memory will be created once a response is generated:
    std::vector<Bid> bidsVector;
    std::vector<Ask> asksVector;

    st::string url = "https://ttblaze.iifl.com/apimarketdata/instruments/quotes";

    if(!curl){
        std::cerr<<"Error Intialising Curl in Quotes() In The Instruments Class"<<std::endl;
    }

    std::string authorisation_header = "authorization: " + token;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, authorisation_header.c_str());

    instruments_json["exchangeSegment"] = exchangeSegment;
    instruments_json["exchangeInstrumentID"] = exchangeInstrumentID;
    
    payload_json["instrument"] = Json::arrayValue;
    payload_json["instrument"].append(instruments_json);
    payload_json["xtsMessageCode"] = xtsMessageCode;
    payload_json["publishFormat"] = publishFormat;

    json_payload_string = Json::writeString(writer, payload_json);

    if(curl){
        // adding url to the curl handle
        curl_easy_setopt(curl,CURLOPT_URL,url.c_str());
        // adding headers to the curl handle
        curl_easy_setopt(curl,CURLOPT_HTTPHEADER,headers);
        // adding json payload string to the curl handle
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,json_payload_string.c_str());
        curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,WriteCallBack);
        curl_easy_setopt(curl,CURLOPT_WRITEDATA,&response_buffer);


        std::cout<<"Peforming A Curl Request To Quotes()"<<std::endl;
        std::string start_curl_event = "Peforming A Curl Request To Quotes()";

        res = curl_easy_perform(curl);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if(response_buffer.empty()){
            std::cerr<<"The response buffer is empty while getting the Quotes()"<<std::endl;
        }

        if(res!=CURLE_OK){
            std::cerr<<"The response buffer is empty while getting the Quotes(): "<<curl_easy_strerror(res)<<std::endl;
            if(res == CURLE_COULDNT_CONNECT){
                std::cerr<<"Couldn't connect to the server. Please check network connection or server address: "<<curl_easy_strerror(res)<<std::endl;
            } else if(res == CURLE_OPERATION_TIMEDOUT){
                std::cerr<<"The request timedout: "<<curl_easy_strerror(res)<<std::endl;
            } else if(res == CURLE_SSL_CONNECT_ERROR){
                std::cerr<<"SSL Connection Error: "<<curl_easy_strerror(res)<<std::endl;
            }
        }

        std::istringstream response_stream(response_buffer);

        if(!Json::parseFromStream(reader,response_stream,&response_data,&errors)){
            std::cerr<<"Error Encountered While Reading the Response Stream from Quotes(): "<<errors<<std::endl;
            if(errors.find("syntax error")!=std::string::npos){
                std::cerr<<"Syntax error detected while Reading the Response Stream from Quotes()"<<std::endl;
            } else {
                std::cerr<<"An Unknown Error Occured"<<std::endl;
            }
        } 

        if(response[0]["description"].find("Get quotes successfully!")!=std::string::npos){
            std::cerr<<"Server Responded With An error"<<std::endl;
        }

        const Json::Value& result = response_data[0]["result"];
        const int mdp = result["mdp"].asInt();
        const int exchangeSegment_response = result["quoteList"]["exchangeSegment"].asInt();
        const int exchangeInstrumentID_response = result["quoteList"]["exchangeInstrumentID"].asInt();


        /* 
        This part deals with unwrapping the listQuotes in response since it is a string format:
        */
        std::string listQuotes = result["listQuotes"].asString();
        std::istringstream list_quotes_stream(listQuotes);
        

        if(!Json::parseFromStream(list_quotes_stream_reader,list_quotes_stream,&list_quotes_data,&list_quotes_errors)){
            std::cerr<<"Error in Reading String Stream of listQuotes from Quotes()"<<std::endl;
        }

        const int messageCode = list_quotes_data["MessageCode"];
        const int ExchangeSegment = list_quotes_data["ExchangeSegment"];
        const int ExchangeInstrumentID = list_quotes_data["ExchangeInstrumentID"];
        const long int ExchangeTimeStamp = list_quotes_data["ExchangeTimeStamp"];

        const Json::Value& bids = list_quotes_data["Bids"];
        const Json::Value& asks = list_quotes_data["Asks"];

        // Getting the Bids:
        bidsVector.reserve(bids.size());
        for(const auto& bid: bids) {
            bidsVector.emplace_back(Bid{
                bid["Size"].asInt(),
                bid["Price"].asDouble(),
                bid["TotalOrders"].asInt(),
                bid["BuyBackMarketMaker"].asInt()
            });
        }

        // Getting the Asks:
        asksVector.reserve(asks.size());
        for(const auto& ask: asks) {
            asksVector.emplace_back(Ask{
                ask["Size"].asInt(),
                ask["Price"].asDouble(),
                ask["TotalOrders"].asInt(), 
                ask["BuyBackMarketMaker"].asInt()});
        }

        if(bidsVector.empty() || asksVector.empty()){
            std::cerr<<"The Bids Or Asks Vector Is Empty"<<std::endl;
        }
        
        auto bidAndAsks = std::make_tuple(bidsVector,asksVector);

        return bidAndAsks;  
    }

}



std::tuple<int,std::vector<std::string>> MarketDataAPIFunctions::IndexList(const int& exchangeSegment, 
                    const std::string& section_header, 
                    const std::string& section_item_name,
                    const std::string& token) {
    
    CURL* curl;
    curl = curl_easy_init();
    if(!curl){
        std::cerr<<"Error intialising curl in IndexList() in MarketDataAPIFunctions class"<<std::endl;
        return;
    }
    CURLcode res;
    std::string response_buffer;


    std::string authorisation = "authorization: " + token;
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, authorisation.c_str());

    // This Initialisaiton of variables is when to send the payload
    std::string json_payload_str;
    Json::Value payload_json;
    payload_json["exchangeSegment"] = exchangeSegment;
    Json::StreamWriterBuilder writer;
    payload_str = Json::writeString(writer,payload);

    // This Initialisaiton of variables is when to send the payload
    Json::Value result;
    Json::CharReaderBuilder response_reader;
    std::string url = "https://ttblaze.iifl.com/apimarketdata/instruments/indexlist";


    if(curl){
        curl_easy_opt(curl, CURLOPT_URL, url);
        curl_easy_opt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_opt(curl, CURLOPT_WRITEFUNCTION, WriteCallBack);
        curl_easy_opt(curl, CURLOPT_WRITEDATA, &response_buffer);
    
        res = curl_easy_perform(curl);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        
        if(response_buffer.empty()){
            throw std::runtime_error("The response buffer is empty while getting the IndexListResponse()");
        }

        if(res!=CURLE_OK){
            std::cerr<<"Bad Request please check the parameters while sending a get request for IndexListResponse()"<<std::endl;
            logEvent("Bad Request please check the parameters while sending a get request for IndexListResponse()");
            if(res == CURLE_COULDNT_CONNECT){
                std::cerr<<"Couldn't connect to the server. Please check network connection or server address: "<<curl_easy_strerror(res)<<std::endl;
            } else if(res == CURLE_OPERATION_TIMEDOUT){
                std::cerr<<"The request timedout: "<<curl_easy_strerror(res)<<std::endl;
            } else if(res == CURLE_SSL_CONNECT_ERROR){
                std::cerr<<"SSL Connection Error: "<<curl_easy_strerror(res)<<std::endl;
            }
        }
        
        Json::CharReaderBuilder reader;
        Json::Value response_data;
        std::string errors;

        std::istringstream res_buff(response_buffer);
        if(!Json::parseFromStream(reader,res_buff,&response_data, &errors)){
            throw std::runtime_error("Error reading buffer into JSON object in IndexList()");
        }

        const Json::Value& result = response_data[0]["result"];
        const int exchangeSegment = result["exchangeSegment"].asInt();

        std::vector<std::string> IndexList;
        for(const auto& index_item: result["indexList"]){
            IndexList.push_back(index_item.asString());
        }

        // Print items to test this function
        std::cout<<"Index: "<<std::endl;
        for(const auto& index_item: IndexList){
            std::cout<<"-"<<index_item<<std::endl;
        }
        std::cout<<"exchangeSegment: "<<exchangeSegment<<std::endl;

        return std::make_tuple(exchangeSegment,IndexList);
    } else {
        std::cerr<<"Something went wrong with IndexListResponse() While Initialising Curl"<<std::endl;
    }

    
}


void MarketDataAPIFunctions::Subscribe(const int& exchangeSegment,
                                        const int& exchangeInstrumentID,
                                        const int& xtsMessageCode,
                                        const int& token){

    CURL* curl = nullptr;
    std::string url = "https://ttblaze.iifl.com/apimarketdata/instruments/subscription";
    std::string response_buffer;
    std::string authorization_header = "authorization" + token;
    struct curl_slist* header = nullptr;

    // This Initialisaiton of variables is when to send the payload
    Json::Value instruments_json, payload_json;
    std::string json_payload_str;
    payload_json["instruments"] = Json::arrayValue;

    // This Initialisaiton of variables is when to send the payload
    Json::Value result;
    Json::Value response_data;
    Json::CharReaderBuilder response_reader;
    std::string errors;
    

    curl = curl_easy_init();

    if(!curl){
        std::cerr<<"Error In Initialising curl MarketDataAPIFunctions::Subscribe()"<<std::endl;
    }

    if(curl){
        headers = curl_easy_append(headers, "Content-Type: application/json");
        headers = curl_easy_append(headers, authorization_header.c_str());
        
        instruments_json["exchangeSegment"] = exchangeSegment;
        instruments_json["exchangeInstrumentID"] = exchangeInstrumentID;

        payload_json["instruments"].append(instruments_json);
        payload_json["xtsMessageCode"] = xtsMessageCode;

        json_payload_str = Json::writeString(writer, payload_json);

        curl_easy_setopt(curl,CURLOPT_URL,url.c_str());
        curl_easy_setopt(curl,CURLOPT_HTTPHEADER,headers);
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,json_payload_str.c_str());
        curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,WriteCallBack);
        curl_easy_setopt(curl,CURLOPT_WRITEDATA,&response_buffer);

        std::cout<<"Peforming A Curl Subscribe()"<<std::endl;
        res = curl_easy_perform(curl);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        std::istringstream res_stream(response_buffer);

        if(!Json::parseFromStream(response_reader,res_stream,&response_data,&errors)){
            std::cerr<<"Error In Storing Data From Response Stream In A JSON Object In Subscribe"<<std::endl;
        }

        const Json::Value& result = response_data[0]["result"];
        std::string list_quotes_string = result["listQuotes"];

        // This part involves conversion of the listQuotes string to a JSON object
        std::istringstream list_quotes_stream(list_quotes_string);
        std::string list_quotes_errors;
        Json::CharReaderBuilder list_quotes_reader_builder;
        Json::Value list_quotes_json_obj;

        if(!Json::parseFromStream(list_quotes_reader_builder,list_quotes_stream,&list_quotes_json_obj,&errors)){
            std::cerr<<"Error in parsing the listQuotes string from the Subscribe function"<<std::endl;
        }

        

    }


}

