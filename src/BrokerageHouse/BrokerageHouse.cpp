/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * 25 July 2006
 */

#include "BrokerageHouse.h"
#include "CommonStructs.h"
#include "DBConnection.h"
#include "TxnHarnessSendToMarket.h"
#include "BrokerVolumeDB.h"
#include "CustomerPositionDB.h"
#include "DataMaintenanceDB.h"
#include "MarketFeedDB.h"
#include "MarketWatchDB.h"
#include "SecurityDetailDB.h"
#include "TradeCleanupDB.h"
#include "TradeLookupDB.h"
#include "TradeOrderDB.h"
#include "TradeResultDB.h"
#include "TradeStatusDB.h"
#include "TradeUpdateDB.h"

void *workerThread(void *data)
{
	try {
		PThreadParameter pThrParam = reinterpret_cast<PThreadParameter>(data);

		CSocket sockDrv;
		sockDrv.setSocketFd(pThrParam->iSockfd); // client socket

		PMsgDriverBrokerage pMessage = new TMsgDriverBrokerage;
		memset(pMessage, 0, sizeof(TMsgDriverBrokerage)); // zero the structure

		TMsgBrokerageDriver Reply; // return message
		INT32 iRet = 0; // transaction return code
		CDBConnection *pDBConnection = NULL;

		// new database connection
		pDBConnection = new CDBConnection(
				pThrParam->pBrokerageHouse->m_szHost,
				pThrParam->pBrokerageHouse->m_szDBName,
				pThrParam->pBrokerageHouse->m_szDBPort,
				pThrParam->pBrokerageHouse->m_szDBPwd,
				pThrParam->pBrokerageHouse->m_szDBUser);
		pDBConnection->setBrokerageHouse(pThrParam->pBrokerageHouse);
		CSendToMarket sendToMarket = CSendToMarket(
				&(pThrParam->pBrokerageHouse->m_fLog));
		CMarketFeedDB marketFeedDB(pDBConnection);
		CMarketFeed marketFeed = CMarketFeed(&marketFeedDB, &sendToMarket);
		CTradeOrderDB tradeOrderDB(pDBConnection);
		CTradeOrder tradeOrder = CTradeOrder(&tradeOrderDB, &sendToMarket);

		// Initialize all classes that will be used to execute transactions.
		CBrokerVolumeDB brokerVolumeDB(pDBConnection);
		CBrokerVolume brokerVolume = CBrokerVolume(&brokerVolumeDB);
		CCustomerPositionDB customerPositionDB(pDBConnection);
		CCustomerPosition customerPosition = CCustomerPosition(&customerPositionDB);
		CMarketWatchDB marketWatchDB(pDBConnection);
		CMarketWatch marketWatch = CMarketWatch(&marketWatchDB);
		CSecurityDetailDB securityDetailDB = CSecurityDetailDB(pDBConnection);
		CSecurityDetail securityDetail = CSecurityDetail(&securityDetailDB);
		CTradeLookupDB tradeLookupDB(pDBConnection);
		CTradeLookup tradeLookup = CTradeLookup(&tradeLookupDB);
		CTradeStatusDB tradeStatusDB(pDBConnection);
		CTradeStatus tradeStatus = CTradeStatus(&tradeStatusDB);
		CTradeUpdateDB tradeUpdateDB(pDBConnection);
		CTradeUpdate tradeUpdate = CTradeUpdate(&tradeUpdateDB);
		CDataMaintenanceDB dataMaintenanceDB(pDBConnection);
		CDataMaintenance dataMaintenance = CDataMaintenance(&dataMaintenanceDB);
		CTradeCleanupDB tradeCleanupDB(pDBConnection);
		CTradeCleanup tradeCleanup = CTradeCleanup(&tradeCleanupDB);
		CTradeResultDB tradeResultDB(pDBConnection);
		CTradeResult tradeResult = CTradeResult(&tradeResultDB);

		do {
			try {
				sockDrv.dbt5Receive(reinterpret_cast<void *>(pMessage),
						sizeof(TMsgDriverBrokerage));
			} catch(CSocketErr *pErr) {
				sockDrv.dbt5Disconnect();

				ostringstream osErr;
				osErr << "Error on Receive: " << pErr->ErrorText() <<
						" at BrokerageHouse::workerThread" << endl;
				pThrParam->pBrokerageHouse->logErrorMessage(osErr.str());
				delete pErr;

				// The socket has been closed, break and let this thread die.
				break;
			}

			try {
				//  Parse Txn type
				switch (pMessage->TxnType) {
				case BROKER_VOLUME:
					iRet = pThrParam->pBrokerageHouse->RunBrokerVolume(
							&(pMessage->TxnInput.BrokerVolumeTxnInput),
							brokerVolume);
					break;
				case CUSTOMER_POSITION:
					iRet = pThrParam->pBrokerageHouse->RunCustomerPosition(
							&(pMessage->TxnInput.CustomerPositionTxnInput),
							customerPosition);
					break;
				case MARKET_FEED:
					iRet = pThrParam->pBrokerageHouse->RunMarketFeed(
							&(pMessage->TxnInput.MarketFeedTxnInput), marketFeed);
					break;
				case MARKET_WATCH:
					iRet = pThrParam->pBrokerageHouse->RunMarketWatch(
							&(pMessage->TxnInput.MarketWatchTxnInput), marketWatch);
					break;
				case SECURITY_DETAIL:
					iRet = pThrParam->pBrokerageHouse->RunSecurityDetail(
							&(pMessage->TxnInput.SecurityDetailTxnInput),
							securityDetail);
					break;
				case TRADE_LOOKUP:
					iRet = pThrParam->pBrokerageHouse->RunTradeLookup(
							&(pMessage->TxnInput.TradeLookupTxnInput), tradeLookup);
					break;
				case TRADE_ORDER:
					iRet = pThrParam->pBrokerageHouse->RunTradeOrder(
							&(pMessage->TxnInput.TradeOrderTxnInput), tradeOrder);
					break;
				case TRADE_RESULT:
					iRet = pThrParam->pBrokerageHouse->RunTradeResult(
							&(pMessage->TxnInput.TradeResultTxnInput), tradeResult);
					break;
				case TRADE_STATUS:
					iRet = pThrParam->pBrokerageHouse->RunTradeStatus(
							&(pMessage->TxnInput.TradeStatusTxnInput),
							tradeStatus);
					break;
				case TRADE_UPDATE:
					iRet = pThrParam->pBrokerageHouse->RunTradeUpdate(
							&(pMessage->TxnInput.TradeUpdateTxnInput), tradeUpdate);
					break;
				case DATA_MAINTENANCE:
					iRet = pThrParam->pBrokerageHouse->RunDataMaintenance(
							&(pMessage->TxnInput.DataMaintenanceTxnInput),
							dataMaintenance);
					break;
				case TRADE_CLEANUP:
					iRet = pThrParam->pBrokerageHouse->RunTradeCleanup(
							&(pMessage->TxnInput.TradeCleanupTxnInput),
							tradeCleanup);
					break;
				default:
					cout << "wrong txn type" << endl;
					iRet = ERR_TYPE_WRONGTXN;
				}
			} catch (const char *str) {
				ostringstream msg;
				msg << time(NULL) << " " << (long long) pthread_self() << " " <<
						szTransactionName[pMessage->TxnType] << endl;
				pThrParam->pBrokerageHouse->logErrorMessage(msg.str());
				iRet = CBaseTxnErr::EXPECTED_ROLLBACK;
			}

			// send status to driver
			Reply.iStatus = iRet;
			try {
				sockDrv.dbt5Send(reinterpret_cast<void *>(&Reply), sizeof(Reply));
			} catch(CSocketErr *pErr) {
				sockDrv.dbt5Disconnect();

				ostringstream osErr;
				osErr << "Error on Send: " << pErr->ErrorText() <<
						" at BrokerageHouse::workerThread" << endl;
				pThrParam->pBrokerageHouse->logErrorMessage(osErr.str());
				delete pErr;

				// The socket has been closed, break and let this thread die.
				break;
			}
		} while (true);

		delete pDBConnection; // close connection with the database
		close(pThrParam->iSockfd); // close socket connection with the driver
		delete pThrParam;
		delete pMessage;
	} catch (CSocketErr *err) {
	}
	return NULL;
}

// entry point for worker thread
void entryWorkerThread(void *data)
{
	PThreadParameter pThrParam = reinterpret_cast<PThreadParameter>(data);

	pthread_t threadID; // thread ID
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_ATTR_INIT);
		}

		// set the detachstate attribute to detached
		status = pthread_attr_setdetachstate(&threadAttribute,
				PTHREAD_CREATE_DETACHED);
		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_ATTR_DETACH);
		}

		// create the thread in the detached state
		status = pthread_create(&threadID, &threadAttribute, &workerThread,
				data);

		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_CREATE);
		}
	} catch(CThreadErr *pErr) {
		// close recently accepted connection, to release driver threads
		close(pThrParam->iSockfd);

		ostringstream osErr;
		osErr << "Error: " << pErr->ErrorText() << " at " <<
				"BrokerageHouse::entryWorkerThread" << endl <<
				"accepted socket connection closed" << endl;
		pThrParam->pBrokerageHouse->logErrorMessage(osErr.str());
		delete pThrParam;
		delete pErr;
	}
}

// Constructor
CBrokerageHouse::CBrokerageHouse(const char *szHost, const char *szDBName,
		const char *szDBPort, const char *szDBPwd, const char *szDBUser,const int iListenPort, char *outputDirectory)
: m_iListenPort(iListenPort)
{
	strncpy(m_szHost, szHost, iMaxHostname);
	m_szHost[iMaxHostname] = '\0';
	strncpy(m_szDBName, szDBName, iMaxDBName);
	m_szDBName[iMaxDBName] = '\0';
	strncpy(m_szDBPort, szDBPort, iMaxPort);
	m_szDBPort[iMaxPort] = '\0';
	strncpy(m_szDBPwd, szDBPwd, iMaxPwd);
	m_szDBPwd[iMaxPwd] = '\0';
	strncpy(m_szDBUser, szDBUser, iMaxUser);
	m_szDBUser[iMaxUser] = '\0';


	char filename[iMaxPath + 1];
	snprintf(filename, iMaxPath, "%s/BrokerageHouse_Error.log",
			outputDirectory);
	m_fLog.open(filename, ios::out);

}

// Destructor
CBrokerageHouse::~CBrokerageHouse()
{
	m_Socket.closeListenerSocket();
	m_fLog.close();
}

void CBrokerageHouse::dumpInputData(PBrokerVolumeTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() << endl;
	for (int i = 0; i < max_broker_list_len; i++) {
		msg << "broker_list[" << i << "] = " << pTxnInput->broker_list[i] <<
				endl;
		msg << "sector_name[" << i << "] = " << pTxnInput->sector_name[i] <<
				endl;
	}
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PCustomerPositionTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() <<
			" CustomerPosition" << endl;
	msg << "acct_id_idx = " << pTxnInput->acct_id_idx << endl;
	msg << "cust_id = " << pTxnInput->cust_id << endl;
	msg << "get_history = " << pTxnInput->get_history << endl;
	msg << "tax_id = " << pTxnInput->tax_id << endl;
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PDataMaintenanceTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() <<
			" DataMaintenance" << endl;
	msg << "acct_id = " << pTxnInput->acct_id << endl;
	msg << "c_id = " << pTxnInput->c_id << endl;
	msg << "co_id = " << pTxnInput->co_id << endl;
	msg << "day_of_month = " << pTxnInput->day_of_month << endl;
	msg << "vol_incr = " << pTxnInput->vol_incr << endl;
	msg << "symbol = " << pTxnInput->symbol << endl;
	msg << "table_name = " << pTxnInput->table_name << endl;
	msg << "tx_id = " << pTxnInput->tx_id << endl;
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PTradeCleanupTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() <<
			" TradeCleanup" << endl;
	msg << "start_trade_id = " << pTxnInput->start_trade_id << endl;
	msg << "st_canceled_id = " << pTxnInput->st_canceled_id << endl;
	msg << "st_pending_id = " << pTxnInput->st_pending_id << endl;
	msg << "st_submitted_id = " << pTxnInput->st_submitted_id << endl;
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PMarketWatchTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() << " MarketWatch" <<
			endl;
	msg << "acct_id = " << pTxnInput->acct_id << endl;
	msg << "c_id = " << pTxnInput->c_id << endl;
	msg << "ending_co_id = " << pTxnInput->ending_co_id << endl;
	msg << "starting_co_id = " << pTxnInput->starting_co_id << endl;
	msg << "start_day = " << pTxnInput->start_day.year << "-" <<
			pTxnInput->start_day.month << "-" <<
			pTxnInput->start_day.day << " " << pTxnInput->start_day.hour <<
			":" << pTxnInput->start_day.minute << ":" <<
			pTxnInput->start_day.second << "." <<
			pTxnInput->start_day.fraction << endl;
	msg << "industry_name = " << pTxnInput->industry_name << endl;
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PMarketFeedTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() << " MarketFeed" <<
			endl;
	msg << "StatusAndTradeType.status_submitted = " <<
			pTxnInput->StatusAndTradeType.status_submitted << endl;
	msg << "StatusAndTradeType.type_limit_buy = " <<
			pTxnInput->StatusAndTradeType.type_limit_buy << endl;
	msg << "StatusAndTradeType.type_limit_sell = " <<
			pTxnInput->StatusAndTradeType .type_limit_sell << endl;
	msg << "StatusAndTradeType.type_stop_loss = " <<
			pTxnInput->StatusAndTradeType .type_stop_loss << endl;
	msg << "zz_padding1 = " << pTxnInput->zz_padding1 << endl;
	msg << "zz_padding2 = " << pTxnInput->zz_padding2 << endl;
	for (int i = 0; i < max_feed_len; i++) {
		msg << "Entries[" << i << "].price_quote = " <<
				pTxnInput->Entries[i].price_quote << endl;
		msg << "Entries[" << i << "].trade_qty = " <<
				pTxnInput->Entries[i].trade_qty << endl;
		msg << "Entries[" << i << "].symbol = " <<
				pTxnInput->Entries[i].symbol << endl;
	}
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PSecurityDetailTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() <<
			" SecurityDetail" << endl;
	msg << "max_rows_to_return = " << pTxnInput->max_rows_to_return << endl;;
	msg << "access_lob_flag = " << pTxnInput->access_lob_flag << endl;;
	msg << "start_day = " << pTxnInput->start_day.year << "-" <<
			pTxnInput->start_day.month << "-" <<
			pTxnInput->start_day.day << " " << pTxnInput->start_day.hour <<
			":" << pTxnInput->start_day.minute << ":" <<
			pTxnInput->start_day.second << "." <<
			pTxnInput->start_day.fraction << endl;
	msg << "symbol = " << pTxnInput->symbol << endl;
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PTradeStatusTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() << " TradeStatus" <<
			endl;
	msg << "acct_id = " << pTxnInput->acct_id << endl;
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PTradeLookupTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() << " TradeLookup" <<
			endl;
	for (int i = 0; i < TradeLookupFrame1MaxRows; i++) {
		msg << "trade_id[" << i << "] = " << pTxnInput->trade_id[i] << endl;
	}
	msg << "acct_id = " << pTxnInput->acct_id << endl;
	msg << "max_acct_id = " << pTxnInput->max_acct_id << endl;
	msg << "frame_to_execute = " << pTxnInput->frame_to_execute << endl;
	msg << "max_trades = " << pTxnInput->max_trades << endl;
	msg << "end_trade_dts = " << pTxnInput->end_trade_dts.year << "-" <<
			pTxnInput->end_trade_dts.month << "-" <<
			pTxnInput->end_trade_dts.day << " " <<
			pTxnInput->end_trade_dts.hour <<
			":" << pTxnInput->end_trade_dts.minute << ":" <<
			pTxnInput->end_trade_dts.second << "." <<
			pTxnInput->end_trade_dts.fraction << endl;
	msg << "start_trade_dts = " << pTxnInput->start_trade_dts.year << "-" <<
			pTxnInput->start_trade_dts.month << "-" <<
			pTxnInput->start_trade_dts.day << " " <<
			pTxnInput->start_trade_dts.hour <<
			":" << pTxnInput->start_trade_dts.minute << ":" <<
			pTxnInput->start_trade_dts.second << "." <<
			pTxnInput->start_trade_dts.fraction << endl;
	msg << "symbol = " << pTxnInput->symbol << endl;
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PTradeOrderTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() << " TradeOrder" <<
			endl;
	msg << "acct_id = " << pTxnInput->acct_id << endl;
	msg << "is_lifo = " << pTxnInput->is_lifo << endl;
	msg << "roll_it_back = " << pTxnInput->roll_it_back << endl;
	msg << "trade_qty = " << pTxnInput->trade_qty << endl;
	msg << "type_is_margin = " << pTxnInput->type_is_margin << endl;
	msg << "co_name = " << pTxnInput->co_name << endl;
	msg << "exec_f_name = " << pTxnInput->exec_f_name << endl;
	msg << "exec_l_name = " << pTxnInput->exec_l_name << endl;
	msg << "exec_tax_id = " << pTxnInput->exec_tax_id << endl;
	msg << "issue = " << pTxnInput->issue << endl;
	msg << "st_pending_id = " << pTxnInput->st_pending_id << endl;
	msg << "st_submitted_id = " << pTxnInput->st_submitted_id << endl;
	msg << "symbol = " << pTxnInput->symbol << endl;
	msg << "trade_type_id = " << pTxnInput->trade_type_id << endl;
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PTradeResultTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() << " TradeResult" <<
			endl;
	msg << "trade_price = " << pTxnInput->trade_price << endl;
	msg << "trade_id = " << pTxnInput->trade_id << endl;
	logErrorMessage(msg.str(), false);
}

void CBrokerageHouse::dumpInputData(PTradeUpdateTxnInput pTxnInput)
{
	ostringstream msg;
	msg << time(NULL) << " " << (long long) pthread_self() << " TradeUpdate" <<
			endl;
	for (int i = 0; i < TradeUpdateFrame1MaxRows; i++) {
		msg << "trade_id[" << i << "] = " << pTxnInput->trade_id[i] << endl;
	}
	msg << "acct_id = " << pTxnInput->acct_id << endl;
	msg << "max_acct_id = " << pTxnInput->max_acct_id << endl;
	msg << "frame_to_execute = " << pTxnInput->frame_to_execute << endl;
	msg << "max_trades = " << pTxnInput->max_trades << endl;
	msg << "trade_id = " << pTxnInput->trade_id << endl;
	msg << "end_trade_dts = " << pTxnInput->end_trade_dts.year << "-" <<
			pTxnInput->end_trade_dts.month << "-" <<
			pTxnInput->end_trade_dts.day << " " <<
			pTxnInput->end_trade_dts.hour <<
			":" << pTxnInput->end_trade_dts.minute << ":" <<
			pTxnInput->end_trade_dts.second << "." <<
			pTxnInput->end_trade_dts.fraction << endl;
	msg << "start_trade_dts = " << pTxnInput->start_trade_dts.year << "-" <<
			pTxnInput->start_trade_dts.month << "-" <<
			pTxnInput->start_trade_dts.day << " " <<
			pTxnInput->start_trade_dts.hour <<
			":" << pTxnInput->start_trade_dts.minute << ":" <<
			pTxnInput->start_trade_dts.second << "." <<
			pTxnInput->start_trade_dts.fraction << endl;
	msg << "symbol = " << pTxnInput->symbol << endl;
	logErrorMessage(msg.str(), false);
}

// Run Broker Volume transaction
INT32 CBrokerageHouse::RunBrokerVolume(PBrokerVolumeTxnInput pTxnInput,
		CBrokerVolume &brokerVolume)
{
	TBrokerVolumeTxnOutput bvOutput;
	memset(&bvOutput, 0, sizeof(TBrokerVolumeTxnOutput));

	try {
		brokerVolume.DoTxn(pTxnInput, &bvOutput);
	} catch (const exception &e) {
		logErrorMessage("BV EXCEPTION\n", false);
		bvOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (bvOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << bvOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return bvOutput.status;
}

// Run Customer Position transaction
INT32 CBrokerageHouse::RunCustomerPosition(PCustomerPositionTxnInput pTxnInput,
		CCustomerPosition &customerPosition)
{
	TCustomerPositionTxnOutput cpOutput;
	memset(&cpOutput, 0, sizeof(TCustomerPositionTxnOutput));

	try {
		customerPosition.DoTxn(pTxnInput, &cpOutput);
	} catch (const exception &e) {
		logErrorMessage("CP EXCEPTION\n", false);
		cpOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (cpOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << cpOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return cpOutput.status;
}

// Run Data Maintenance transaction
INT32 CBrokerageHouse::RunDataMaintenance(PDataMaintenanceTxnInput pTxnInput,
		CDataMaintenance &dataMaintenance)
{
	TDataMaintenanceTxnOutput dmOutput;
	memset(&dmOutput, 0, sizeof(TDataMaintenanceTxnOutput));

	try {
		dataMaintenance.DoTxn(pTxnInput, &dmOutput);
	} catch (const exception &e) {
		logErrorMessage("DM EXCEPTION\n", false);
		dmOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (dmOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << dmOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return dmOutput.status;
}

// Run Trade Cleanup transaction
INT32 CBrokerageHouse::RunTradeCleanup(PTradeCleanupTxnInput pTxnInput,
		CTradeCleanup &tradeCleanup)
{
	TTradeCleanupTxnOutput tcOutput;
	memset(&tcOutput, 0, sizeof(TTradeCleanupTxnOutput));

	try {
		tradeCleanup.DoTxn(pTxnInput, &tcOutput);
	} catch (const exception &e) {
		logErrorMessage("TC EXCEPTION\n", false);
		tcOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (tcOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << tcOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return tcOutput.status;
}

// Run Market Feed transaction
INT32 CBrokerageHouse::RunMarketFeed(PMarketFeedTxnInput pTxnInput,
		CMarketFeed &marketFeed)
{
	TMarketFeedTxnOutput mfOutput;
	memset(&mfOutput, 0, sizeof(TMarketFeedTxnOutput));

	try {
		marketFeed.DoTxn(pTxnInput, &mfOutput);
	} catch (const exception &e) {
		logErrorMessage("MF EXCEPTION\n", false);
		mfOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (mfOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << mfOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return mfOutput.status;
}

// Run Market Watch transaction
INT32 CBrokerageHouse::RunMarketWatch(PMarketWatchTxnInput pTxnInput,
		CMarketWatch &marketWatch)
{
	TMarketWatchTxnOutput mwOutput;
	memset(&mwOutput, 0, sizeof(TMarketWatchTxnOutput));

	try {
		marketWatch.DoTxn(pTxnInput, &mwOutput);
	} catch (const exception &e) {
		logErrorMessage("MW EXCEPTION\n", false);
		mwOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (mwOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << mwOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return mwOutput.status;
}

// Run Security Detail transaction
INT32 CBrokerageHouse::RunSecurityDetail(PSecurityDetailTxnInput pTxnInput,
		CSecurityDetail &securityDetail)
{
	TSecurityDetailTxnOutput sdOutput;
	memset(&sdOutput, 0, sizeof(TSecurityDetailTxnOutput));

	try {
		securityDetail.DoTxn(pTxnInput, &sdOutput);
	} catch (const exception &e) {
		logErrorMessage("SD EXCEPTION\n", false);
		sdOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (sdOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << sdOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return sdOutput.status;
}

// Run Trade Lookup transaction
INT32 CBrokerageHouse::RunTradeLookup(PTradeLookupTxnInput pTxnInput,
		CTradeLookup &tradeLookup)
{
	TTradeLookupTxnOutput tlOutput;
	memset(&tlOutput, 0, sizeof(TTradeLookupTxnOutput));

	try {
		tradeLookup.DoTxn(pTxnInput, &tlOutput);
	} catch (const exception &e) {
		logErrorMessage("TL EXCEPTION\n", false);
		tlOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (tlOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << tlOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return tlOutput.status;
}

// Run Trade Order transaction
INT32 CBrokerageHouse::RunTradeOrder(PTradeOrderTxnInput pTxnInput,
		CTradeOrder &tradeOrder)
{
	TTradeOrderTxnOutput toOutput;
	memset(&toOutput, 0, sizeof(TTradeOrderTxnOutput));

	try {
		tradeOrder.DoTxn(pTxnInput, &toOutput);
	} catch (const exception &e) {
		toOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (toOutput.status != CBaseTxnErr::SUCCESS && 
	    !(toOutput.status == CBaseTxnErr::EXPECTED_ROLLBACK && pTxnInput->roll_it_back)) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << toOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return toOutput.status;
}

// Run Trade Result transaction
INT32 CBrokerageHouse::RunTradeResult(PTradeResultTxnInput pTxnInput,
		CTradeResult &tradeResult)
{
	TTradeResultTxnOutput trOutput;
	memset(&trOutput, 0, sizeof(TTradeResultTxnOutput));

	try {
		tradeResult.DoTxn(pTxnInput, &trOutput);
	} catch (const exception &e) {
		logErrorMessage("TR EXCEPTION\n", false);
		trOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (trOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << trOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return trOutput.status;
}

// Run Trade Status transaction
INT32 CBrokerageHouse::RunTradeStatus(PTradeStatusTxnInput pTxnInput,
		CTradeStatus &tradeStatus)
{
	TTradeStatusTxnOutput tsOutput;
	memset(&tsOutput, 0, sizeof(TTradeStatusTxnOutput));

	try {
		tradeStatus.DoTxn(pTxnInput, &tsOutput);
	} catch (const exception &e) {
		logErrorMessage("TS EXCEPTION\n", false);
		tsOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (tsOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << tsOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return tsOutput.status;
}

// Run Trade Update transaction
INT32 CBrokerageHouse::RunTradeUpdate(PTradeUpdateTxnInput pTxnInput,
		CTradeUpdate &tradeUpdate)
{
	TTradeUpdateTxnOutput tuOutput;
	memset(&tuOutput, 0, sizeof(TTradeUpdateTxnOutput));

	try {
		tradeUpdate.DoTxn(pTxnInput, &tuOutput);
	} catch (const exception &e) {
		logErrorMessage("TU EXCEPTION\n", false);
		tuOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (tuOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << tuOutput.status << endl;
		logErrorMessage(msg.str(), false);
		dumpInputData(pTxnInput);
	}
	return tuOutput.status;
}

// Listener
void CBrokerageHouse::startListener(void)
{
	int acc_socket;
	PThreadParameter pThrParam = NULL;

	m_Socket.dbt5Listen(m_iListenPort);

	while (true) {
		acc_socket = 0;
		try {
			acc_socket = m_Socket.dbt5Accept();

			pThrParam = new TThreadParameter;
			// zero the structure
			memset(pThrParam, 0, sizeof(TThreadParameter));

			pThrParam->iSockfd = acc_socket;
			pThrParam->pBrokerageHouse = this;

			// call entry point
			mythreadPool->add(workerThread,reinterpret_cast<void *>(pThrParam));
			entryWorkerThread(reinterpret_cast<void *>(pThrParam));

		} catch (CSocketErr *pErr) {
			ostringstream osErr;
			osErr << "Problem accepting socket connection" << endl <<
					"Error: " << pErr->ErrorText() << " at " <<
					"BrokerageHouse::Listener" << endl;
			logErrorMessage(osErr.str());
			delete pErr;
			delete pThrParam;
		}
	}
}

// logErrorMessage
void CBrokerageHouse::logErrorMessage(const string sErr, bool bScreen)
{
	m_LogLock.lock();
	if (bScreen) cout << sErr;
	m_fLog << sErr;
	m_fLog.flush();
	m_LogLock.unlock();
}
