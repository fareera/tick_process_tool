// TestDFITCSecApi.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//
#pragma warning(disable: 4355)
#pragma warning(disable: 4996)

#include "datamodel.h"

#include <iostream>
#include <atomic>
#include <chrono>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>

#include "DFITCSECMdApi.h"
#include "sqlite3_helper.h"
#include "unicode.h"
#include "timecvt.h"
#include "fmt/format.h"

//
class MySpi : public DFITCSECMdSpi {
public:
    MySpi(DFITCSECMdApi* api) : api_(api) {
        dbhelper_ = Sqlite3Helper::get_instance();
        dbhelper_->init("D:\\repo\\LTGW\\ltgw.db3");

        stk_out_ = std::ofstream(fmt::format("stk_{0}.csv", calendar_day()), std::ios::out);
        stk_opt_out_ = std::ofstream(fmt::format("stk_opt_{0}.csv", calendar_day()), std::ios::out);
    }
public:
    virtual void OnFrontConnected() override;
    virtual void OnFrontDisconnected(int nReason) override;

    virtual void OnRtnNotice(DFITCSECRspNoticeField* pNotice) override;
    virtual void OnRspStockAvailableQuot(struct DFITCRspQuotQryField* pAvailableQuotInfo, struct DFITCSECRspInfoField* pRspInfo, bool flag) override;
    virtual void OnRspSopAvailableQuot(struct DFITCRspQuotQryField* pAvailableQuotInfo, struct DFITCSECRspInfoField* pRspInfo, bool flag) override;
    virtual void OnRspStockUserLogin(struct DFITCSECRspUserLoginField* pRspUserLogin, struct DFITCSECRspInfoField* pRspInfo) override;
    virtual void OnRspStockSubMarketData(struct DFITCSECSpecificInstrumentField* pSpecificInstrument, struct DFITCSECRspInfoField* pRspInfo) override;
    virtual void OnRspSOPUserLogin(struct DFITCSECRspUserLoginField* pRspUserLogin, struct DFITCSECRspInfoField* pRspInfo) override;
    virtual void OnRspSOPSubMarketData(struct DFITCSECSpecificInstrumentField* pSpecificInstrument, struct DFITCSECRspInfoField* pRspInfo) override;

    virtual void OnStockMarketData(struct DFITCStockDepthMarketDataField* pMarketDataField) override;
    virtual void OnSOPMarketData(struct DFITCSOPDepthMarketDataField* pMarketDataField) override;

public:
    bool is_symbol_ready();
    void load_and_update_symbol();
    void subscribe_all_symbols();

private:
    DFITCSECMdApi* api_;
    std::atomic<int64_t> req_id_ = 0;
    int stock_symbol_count_ = 0;
    int sop_symbol_count_ = 0;
    std::vector<Symbol> stock_symbol_list_;
    std::vector<Symbol> stock_option_symbol_list_;

    Sqlite3Helper* dbhelper_;

    std::ofstream stk_out_;
    std::ofstream stk_opt_out_;
};


void MySpi::OnFrontConnected() {
    std::cout << __FUNCTION__ << std::endl;

    DFITCSECReqUserLoginField field = { 0 };
    field.requestID = ++req_id_;
    strcpy(field.accountID, "200000620938");
    strcpy(field.password, "200000620938");
    // field.compressflag = DFITCSEC_COMPRESS_TRUE;
    api_->ReqStockUserLogin(&field);

    DFITCSECReqUserLoginField field1 = { 0 };
    field1.requestID = ++req_id_;
    strcpy(field1.accountID, "200000620938");
    strcpy(field1.password, "200000620938");
    api_->ReqSOPUserLogin(&field);
}

void MySpi::OnFrontDisconnected(int nReason) {
    std::cout << __FUNCTION__ << std::endl;

}


void MySpi::OnRtnNotice(DFITCSECRspNoticeField* pNotice) {
    std::cout << __FUNCTION__ << std::endl;
}

void MySpi::OnRspStockAvailableQuot(struct DFITCRspQuotQryField* pAvailableQuotInfo, struct DFITCSECRspInfoField* pRspInfo, bool flag) {
    std::cout << __FUNCTION__ << std::endl;

    stock_symbol_count_++;

    std::cout << "Total available stock symbol: " << stock_symbol_count_  << std::endl;

    stock_symbol_list_.push_back({-1, pAvailableQuotInfo->exchangeID, pAvailableQuotInfo->securityID, gbk_to_utf8(pAvailableQuotInfo->securityName), 0 });
}

void MySpi::OnRspSopAvailableQuot(struct DFITCRspQuotQryField* pAvailableQuotInfo, struct DFITCSECRspInfoField* pRspInfo, bool flag) {
    std::cout << __FUNCTION__ << std::endl;

    sop_symbol_count_++;

    std::cout << "Total available stock option symbol: " << sop_symbol_count_ << std::endl;

    stock_option_symbol_list_.push_back({-1, pAvailableQuotInfo->exchangeID, pAvailableQuotInfo->securityID, gbk_to_utf8(pAvailableQuotInfo->securityName), 3 });
}



void MySpi::OnRspStockUserLogin(struct DFITCSECRspUserLoginField* pRspUserLogin, struct DFITCSECRspInfoField* pRspInfo) {
    std::cout << __FUNCTION__ << std::endl;


}


void MySpi::OnRspStockSubMarketData(struct DFITCSECSpecificInstrumentField* pSpecificInstrument, struct DFITCSECRspInfoField* pRspInfo) {
    // std::cout << __FUNCTION__ << std::endl;

}

void MySpi::OnRspSOPUserLogin(struct DFITCSECRspUserLoginField* pRspUserLogin, struct DFITCSECRspInfoField* pRspInfo) {
    std::cout << __FUNCTION__ << std::endl;
}


void MySpi::OnRspSOPSubMarketData(struct DFITCSECSpecificInstrumentField* pSpecificInstrument, struct DFITCSECRspInfoField* pRspInfo) {
    std::cout << __FUNCTION__ << std::endl;

}


void MySpi::OnStockMarketData(struct DFITCStockDepthMarketDataField* pMarketDataField) {
    //std::cout << __FUNCTION__ << std::endl;
    std::cout << "[" << pMarketDataField->sharedDataField.updateTime << "] "
        << pMarketDataField->staticDataField.securityID << " " << pMarketDataField->staticDataField.securityName
        << " @ "
        << pMarketDataField->sharedDataField.latestPrice
        << std::endl;

    stk_out_
<< pMarketDataField->specificDataField.peRadio1 << ","
    << pMarketDataField->specificDataField.peRadio2 << ","
    << pMarketDataField->staticDataField.securityID << ","
    << pMarketDataField->staticDataField.securityName << ","
    << pMarketDataField->staticDataField.tradingDay << ","
    << pMarketDataField->staticDataField.exchangeID << ","
    << pMarketDataField->staticDataField.preClosePrice << ","
    << pMarketDataField->staticDataField.openPrice << ","
    << pMarketDataField->staticDataField.upperLimitPrice << ","
    << pMarketDataField->staticDataField.lowerLimitPrice << ","
    << pMarketDataField->sharedDataField.latestPrice << ","
    << pMarketDataField->sharedDataField.turnover << ","
    << pMarketDataField->sharedDataField.highestPrice << ","
    << pMarketDataField->sharedDataField.lowestPrice << ","
    << pMarketDataField->sharedDataField.tradeQty << ","
    << pMarketDataField->sharedDataField.updateTime << ","
    << pMarketDataField->sharedDataField.bidPrice1 << ","
    << pMarketDataField->sharedDataField.bidQty1 << ","
    << pMarketDataField->sharedDataField.askPrice1 << ","
    << pMarketDataField->sharedDataField.askQty1 << ","
    << pMarketDataField->sharedDataField.bidPrice2 << ","
    << pMarketDataField->sharedDataField.bidQty2 << ","
    << pMarketDataField->sharedDataField.askPrice2 << ","
    << pMarketDataField->sharedDataField.askQty2 << ","
    << pMarketDataField->sharedDataField.bidPrice3 << ","
    << pMarketDataField->sharedDataField.bidQty3 << ","
    << pMarketDataField->sharedDataField.askPrice3 << ","
    << pMarketDataField->sharedDataField.askQty3 << ","
    << pMarketDataField->sharedDataField.bidPrice4 << ","
    << pMarketDataField->sharedDataField.bidQty4 << ","
    << pMarketDataField->sharedDataField.askPrice4 << ","
    << pMarketDataField->sharedDataField.askQty4 << ","
    << pMarketDataField->sharedDataField.bidPrice5 << ","
    << pMarketDataField->sharedDataField.bidQty5 << ","
    << pMarketDataField->sharedDataField.askPrice5 << ","
    << pMarketDataField->sharedDataField.askQty5 << ","
    << pMarketDataField->sharedDataField.tradingPhaseCode << "\n";

            
}

void MySpi::OnSOPMarketData(struct DFITCSOPDepthMarketDataField* pMarketDataField) {
    //std::cout << __FUNCTION__ << std::endl;
    std::cout << "[" << pMarketDataField->sharedDataField.updateTime << "] "
        << pMarketDataField->staticDataField.securityID << " " << pMarketDataField->staticDataField.securityName
        << " @ "
        << pMarketDataField->sharedDataField.latestPrice
        << std::endl;

    stk_opt_out_ 
<< pMarketDataField->specificDataField.auctionPrice << ","
    << pMarketDataField->specificDataField.contractID << ","
    << pMarketDataField->specificDataField.execPrice << ","
    << pMarketDataField->specificDataField.latestEnquiryTime << ","
    << pMarketDataField->specificDataField.positionQty << ","
    << pMarketDataField->specificDataField.preSettlePrice << ","
    << pMarketDataField->specificDataField.settlePrice << ","
    << pMarketDataField->staticDataField.securityID << ","
    << pMarketDataField->staticDataField.securityName << ","
    << pMarketDataField->staticDataField.tradingDay << ","
    << pMarketDataField->staticDataField.exchangeID << ","
    << pMarketDataField->staticDataField.preClosePrice << ","
    << pMarketDataField->staticDataField.openPrice << ","
    << pMarketDataField->staticDataField.upperLimitPrice << ","
    << pMarketDataField->staticDataField.lowerLimitPrice << ","
    << pMarketDataField->sharedDataField.latestPrice << ","
    << pMarketDataField->sharedDataField.turnover << ","
    << pMarketDataField->sharedDataField.highestPrice << ","
    << pMarketDataField->sharedDataField.lowestPrice << ","
    << pMarketDataField->sharedDataField.tradeQty << ","
    << pMarketDataField->sharedDataField.updateTime << ","
    << pMarketDataField->sharedDataField.bidPrice1 << ","
    << pMarketDataField->sharedDataField.bidQty1 << ","
    << pMarketDataField->sharedDataField.askPrice1 << ","
    << pMarketDataField->sharedDataField.askQty1 << ","
    << pMarketDataField->sharedDataField.bidPrice2 << ","
    << pMarketDataField->sharedDataField.bidQty2 << ","
    << pMarketDataField->sharedDataField.askPrice2 << ","
    << pMarketDataField->sharedDataField.askQty2 << ","
    << pMarketDataField->sharedDataField.bidPrice3 << ","
    << pMarketDataField->sharedDataField.bidQty3 << ","
    << pMarketDataField->sharedDataField.askPrice3 << ","
    << pMarketDataField->sharedDataField.askQty3 << ","
    << pMarketDataField->sharedDataField.bidPrice4 << ","
    << pMarketDataField->sharedDataField.bidQty4 << ","
    << pMarketDataField->sharedDataField.askPrice4 << ","
    << pMarketDataField->sharedDataField.askQty4 << ","
    << pMarketDataField->sharedDataField.bidPrice5 << ","
    << pMarketDataField->sharedDataField.bidQty5 << ","
    << pMarketDataField->sharedDataField.askPrice5 << ","
    << pMarketDataField->sharedDataField.askQty5 << ","
    << pMarketDataField->sharedDataField.tradingPhaseCode << "\n";
}

bool MySpi::is_symbol_ready() {
    int tradingday = calendar_day();
    int num_symbols = 0;
    if (dbhelper_->fetch_one_or_none(num_symbols, fmt::format("select count(1) from symbol where tradingday = '{0}'", tradingday).c_str())) {
        if (num_symbols > 0) {
            return true;
        }
    }

    return false;
}


void MySpi::load_and_update_symbol() {
    DFITCReqQuotQryField field = { 0 };
    strcpy(field.accountID, "20000020938");
    //strcpy(field.exchangeID, "SH");
    field.requestID = ++req_id_;

    api_->ReqStockAvailableQuotQry(&field);


    DFITCReqQuotQryField field1 = { 0 };
    strcpy(field1.accountID, "20000020938");
    //strcpy(field1.exchangeID, "SZ");
    field1.requestID = ++req_id_;

    api_->ReqSopAvailableQuotQry(&field1);

    std::this_thread::sleep_for(std::chrono::seconds(20));

    dbhelper_->update_symbol(stock_symbol_list_);
    dbhelper_->update_symbol(stock_option_symbol_list_);
}

void MySpi::subscribe_all_symbols() {
    std::vector<Symbol> stock_sym_list;
    std::string sql = fmt::format("select * from symbol where tradingday = {0} and type = 0", calendar_day());
    dbhelper_->fetch_rows(stock_sym_list, sql.c_str());

    std::cout << "Fetched " << stock_sym_list.size() << " stock symbols from the db" << std::endl;

    char **stock_sub_list = new char*[stock_sym_list.size()];

    std::string sym;
    std::vector<std::string> sym_list;
    sym_list.reserve(stock_sym_list.size());
    for (int i = 0; i < stock_sym_list.size(); ++i) {
        sym = fmt::format("{0}{1}", stock_sym_list[i].exchange_id, stock_sym_list[i].instrument_id);
        sym_list.push_back(sym);
        stock_sub_list[i] = (char*)sym_list[i].c_str();
    }
    api_->SubscribeStockMarketData(stock_sub_list, stock_sym_list.size(), ++req_id_);
    

    std::vector<Symbol> stock_opt_list;
    sql = fmt::format("select * from symbol where tradingday = {0} and type = 3", calendar_day());
    dbhelper_->fetch_rows(stock_opt_list, sql.c_str());

    std::cout << "Fetched " << stock_opt_list.size() << " stock option symbols from the db" << std::endl;

    char** stk_opt_sub_list = new char* [stock_opt_list.size()];

    sym_list.clear();
    sym_list.reserve(stock_opt_list.size());
    for (int i = 0; i < stock_opt_list.size(); ++i) {
        sym = fmt::format("{0}{1}", stock_opt_list[i].exchange_id, stock_opt_list[i].instrument_id);
        sym_list.push_back(sym);
        stk_opt_sub_list[i] = (char*)sym_list[i].c_str();
    }
   
    int i = api_->SubscribeSOPMarketData(stk_opt_sub_list, stock_opt_list.size(), ++req_id_);



}



int main()
{
    DFITCSECMdApi* api = DFITCSECMdApi::CreateDFITCMdApi();
    MySpi gw(api);

    api->Init("tcp://211.95.87.149:10215", &gw);

    std::this_thread::sleep_for(std::chrono::seconds(1));

    if (!gw.is_symbol_ready()) {
        gw.load_and_update_symbol();
    }

    // Subscribe all symbols
    gw.subscribe_all_symbols();
    

    getchar();
    getchar();
}



