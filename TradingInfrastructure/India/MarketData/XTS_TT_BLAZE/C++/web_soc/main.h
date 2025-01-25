#ifndef MAIN_H
#define MAIN_H

#include <iostream>
#include <string>
#include <thread>
#include <functional>
#include <map>


#include <include/websocketpp/config/asio_client.hpp>
#include <include/websocketpp/client.hpp>


class WebSocketClient{
    public:
        WebSocketClient(const std::string &token,
                        const std::string &userID,
                        const std::string &publishFormat,
                        const std::string &broadcastMode);
        ~WebSocketClient();

        void start();
        void handle_connect();
        void handle_connect_error(const std::string& error);
        void handle_disconnect();
        void handle_data_event(const std::string& event, const std::string& data);

    public:
        websocketpp::       

}