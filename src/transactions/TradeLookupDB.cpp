#include "TradeLookupDB.h"

using namespace TPCE;

CTradeLookupDB::CTradeLookupDB(CDBConnection *pDBConn)
		: CTxnDBBase(pDBConn)
{
}

CTradeLookupDB::~CTradeLookupDB()
{
}

void CTradeLookupDB::DoTradeLookupFrame1(const TTradeLookupFrame1Input *pIn,
										 TTradeLookupFrame1Output *pOut)
{
	SQLHSTMT stmt;

	SQLRETURN rc;

	unsigned char is_cash; //SQL_C_BIT
	unsigned char is_market; //SQL_C_BIT

	char date_buf[15]; //pOut->trade_info[].settlement_cash_due_date
	char datetime_buf[30]; //pOut->trade_info[].cash_transaction_dts pOut->trade_info[].trade_history_dts[]

	char trade_history_status_id[cTH_ST_ID_len+1];


	stmt = m_Stmt;
	rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	BeginTxn();


	pOut->num_found = 0;

	for (int i = 0; i < pIn->max_trades; i++)
	{
		/* SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt,
				  t_trade_price
		   FROM trade, trade_type
		   WHERE t_id = %ld
			 AND t_tt_id = tt_id */


		stmt = m_Stmt;
		ostringstream osTLF1_1;
		osTLF1_1 << "SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt, t_trade_price FROM trade, trade_type WHERE t_id = " <<
		pIn->trade_id[i] << " AND t_tt_id = tt_id";
		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF1_1.str().c_str()), SQL_NTS);

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].bid_price), 0, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 2, SQL_C_CHAR, pOut->trade_info[i].exec_name, cEXEC_NAME_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 3, SQL_C_BIT, &is_cash, 0, NULL); //pOut->trade_info[i].is_cash
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 4, SQL_C_BIT, &is_market, 0, NULL); //pOut->trade_info[i].is_market
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 5, SQL_C_DOUBLE, &(pOut->trade_info[i].trade_price), 0, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		SQLCloseCursor(stmt);

		if (rc != SQL_NO_DATA_FOUND) {
			pOut->num_found++;
		} else {
			continue;
		}

		pOut->trade_info[i].is_cash = (is_cash != 0);
		pOut->trade_info[i].is_market = (is_market != 0);


		/* SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type
		   FROM settlement
		   WHERE se_t_id = %ld */


		stmt = m_Stmt;
		ostringstream osTLF1_2;
		osTLF1_2 << "SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = " <<

		pIn->trade_id[i];
		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF1_2.str().c_str()), SQL_NTS);

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].settlement_amount), 0, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 2, SQL_C_CHAR, date_buf, 15, NULL); //pOut->trade_info[].settlement_cash_due_date
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].settlement_cash_type, cSE_CASH_TYPE_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		SQLCloseCursor(stmt);

		//pOut->trade_info[i].settlement_cash_due_date = date_buf
		sscanf(date_buf,"%4hd-%2hu-%2hu",
			   &(pOut->trade_info[i].settlement_cash_due_date.year),
			   &(pOut->trade_info[i].settlement_cash_due_date.month),
			   &(pOut->trade_info[i].settlement_cash_due_date.day));
		pOut->trade_info[i].settlement_cash_due_date.hour
				= pOut->trade_info[i].settlement_cash_due_date.minute
				= pOut->trade_info[i].settlement_cash_due_date.second
				= 0;
		pOut->trade_info[i].settlement_cash_due_date.fraction = 0;


		if(pOut->trade_info[i].is_cash)
		{
			/* SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name
			   FROM cash_transaction
			   WHERE ct_t_id = %ld */

			stmt = m_Stmt;
			ostringstream osTLF1_3;
			osTLF1_3 << "SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = " <<

			pIn->trade_id[i];
			rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF1_3.str().c_str()), SQL_NTS);


			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].cash_transaction_amount), 0, NULL);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLBindCol(stmt, 2, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->trade_info[].cash_transaction_dts
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].cash_transaction_name, cCT_NAME_len+1, NULL);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLFetch(stmt);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
				ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			SQLCloseCursor(stmt);

			//pOut->trade_info[i].cash_transaction_dts = datetime_buf
			sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
				   &(pOut->trade_info[i].cash_transaction_dts.year),
				   &(pOut->trade_info[i].cash_transaction_dts.month),
				   &(pOut->trade_info[i].cash_transaction_dts.day),
				   &(pOut->trade_info[i].cash_transaction_dts.hour),
				   &(pOut->trade_info[i].cash_transaction_dts.minute),
				   &(pOut->trade_info[i].cash_transaction_dts.second),
				   &(pOut->trade_info[i].cash_transaction_dts.fraction));
			pOut->trade_info[i].cash_transaction_dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
		}


		/* SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id
		   FROM trade_history
		   WHERE th_t_id = %ld
		   ORDER BY th_dts
		   LIMIT 3 */


		stmt = m_Stmt;
		ostringstream osTLF1_4;
		osTLF1_4 << "SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = " <<
	    pIn->trade_id[i] << " ORDER BY th_dts LIMIT 3";
		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF1_4.str().c_str()), SQL_NTS);


		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 1, SQL_C_CHAR, datetime_buf, 30, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_history_status_id, cTH_ST_ID_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

		int j = 0;
		while(rc != SQL_NO_DATA_FOUND && j < 3)
		{
			//pOut->trade_info[i].trade_history_dts[j] = datetime_buf
			sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
				   &(pOut->trade_info[i].trade_history_dts[j].year),
				   &(pOut->trade_info[i].trade_history_dts[j].month),
				   &(pOut->trade_info[i].trade_history_dts[j].day),
				   &(pOut->trade_info[i].trade_history_dts[j].hour),
				   &(pOut->trade_info[i].trade_history_dts[j].minute),
				   &(pOut->trade_info[i].trade_history_dts[j].second),
				   &(pOut->trade_info[i].trade_history_dts[j].fraction));
			pOut->trade_info[i].trade_history_dts[j].fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					trade_history_status_id, cTH_ST_ID_len+1);
			j++;

			rc = SQLFetch(stmt);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
				ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		}
		SQLCloseCursor(stmt);
	}

	CommitTxn();

	//pOut->status = CBaseTxnErr::SUCCESS;
}

void CTradeLookupDB::DoTradeLookupFrame2(const TTradeLookupFrame2Input *pIn,
										 TTradeLookupFrame2Output *pOut)
{
	SQLHSTMT stmt;

	SQLRETURN rc;

	char start_trade_dts[30]; //pIn->start_trade_dts
	char end_trade_dts[30]; //pIn->end_trade_dts

	TTradeLookupFrame2TradeInfo trade_info;
	unsigned char is_cash; //SQL_C_BIT

	char date_buf[15]; //pOut->trade_info[].settlement_cash_due_date
	char datetime_buf[30]; //pOut->trade_info[].cash_transaction_dts pOut->trade_info[].trade_history_dts[]

	char trade_history_status_id[cTH_ST_ID_len+1];

	int i;

	stmt = m_Stmt;
	rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	BeginTxn();

	/* SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price
	   FROM trade
	   WHERE t_ca_id = %ld
		 AND t_dts >= '%s'
		 AND t_dts <= '%s'
	   ORDER BY t_dts
	   LIMIT pIn->max_trades */

	snprintf(start_trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
			 pIn->start_trade_dts.year,
			 pIn->start_trade_dts.month,
			 pIn->start_trade_dts.day,
			 pIn->start_trade_dts.hour,
			 pIn->start_trade_dts.minute,
			 pIn->start_trade_dts.second,
			 pIn->start_trade_dts.fraction / 1000); //nano -> micro
	snprintf(end_trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
			 pIn->end_trade_dts.year,
			 pIn->end_trade_dts.month,
			 pIn->end_trade_dts.day,
			 pIn->end_trade_dts.hour,
			 pIn->end_trade_dts.minute,
			 pIn->end_trade_dts.second,
			 pIn->end_trade_dts.fraction / 1000); //nano -> micro


	stmt = m_Stmt;
	ostringstream osTLF2_1;

	osTLF2_1 << "SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price FROM trade WHERE t_ca_id = " <<
	pIn->acct_id << " AND t_dts >= '" <<
	start_trade_dts << "' AND t_dts <= '" <<
	end_trade_dts << "' ORDER BY t_dts LIMIT " <<
	pIn->max_trades;

	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF2_1.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(trade_info.bid_price), 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_info.exec_name, cEXEC_NAME_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 3, SQL_C_BIT, &is_cash, 0, NULL); //pOut->trade_info[i].is_cash
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	rc = SQLBindCol(stmt, 4, SQL_C_SBIGINT, &(trade_info.trade_id), 0, NULL);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 5, SQL_C_DOUBLE, &(trade_info.trade_price), 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	i = 0;
	while(rc != SQL_NO_DATA_FOUND && i < pIn->max_trades)
	{
		pOut->trade_info[i].bid_price = trade_info.bid_price;
		strncpy(pOut->trade_info[i].exec_name, trade_info.exec_name, cEXEC_NAME_len+1);
		pOut->trade_info[i].is_cash = (is_cash != 0);
		pOut->trade_info[i].trade_id = trade_info.trade_id;
		pOut->trade_info[i].trade_price = trade_info.trade_price;
		i++;

		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	}
	SQLCloseCursor(stmt);

	pOut->num_found = i;


	for (i = 0; i < pOut->num_found; i++)
	{
		/* SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type
		   FROM settlement
		   WHERE se_t_id = %s */
		stmt = m_Stmt;
		ostringstream osTLF2_2;

		osTLF2_2 << "SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = " <<

		pOut->trade_info[i].trade_id;
		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF2_2.str().c_str()), SQL_NTS);


		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].settlement_amount), 0, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 2, SQL_C_CHAR, date_buf, 15, NULL); //pOut->trade_info[].settlement_cash_due_date
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].settlement_cash_type, cSE_CASH_TYPE_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		SQLCloseCursor(stmt);

		//pOut->trade_info[i].settlement_cash_due_date = date_buf
		sscanf(date_buf,"%4hd-%2hu-%2hu",
			   &(pOut->trade_info[i].settlement_cash_due_date.year),
			   &(pOut->trade_info[i].settlement_cash_due_date.month),
			   &(pOut->trade_info[i].settlement_cash_due_date.day));
		pOut->trade_info[i].settlement_cash_due_date.hour
				= pOut->trade_info[i].settlement_cash_due_date.minute
				= pOut->trade_info[i].settlement_cash_due_date.second
				= 0;
		pOut->trade_info[i].settlement_cash_due_date.fraction = 0;


		if(pOut->trade_info[i].is_cash)
		{
			/* SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name
			   FROM cash_transaction
			   WHERE ct_t_id = %s */

			stmt = m_Stmt;
			ostringstream osTLF2_3;

			osTLF2_3 << "SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = " <<

			pOut->trade_info[i].trade_id;
			rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF2_3.str().c_str()), SQL_NTS);


			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].cash_transaction_amount), 0, NULL);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLBindCol(stmt, 2, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->trade_info[].cash_transaction_dts
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].cash_transaction_name, cCT_NAME_len+1, NULL);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLFetch(stmt);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
				ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			SQLCloseCursor(stmt);

			//pOut->trade_info[i].cash_transaction_dts = datetime_buf
			sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
				   &(pOut->trade_info[i].cash_transaction_dts.year),
				   &(pOut->trade_info[i].cash_transaction_dts.month),
				   &(pOut->trade_info[i].cash_transaction_dts.day),
				   &(pOut->trade_info[i].cash_transaction_dts.hour),
				   &(pOut->trade_info[i].cash_transaction_dts.minute),
				   &(pOut->trade_info[i].cash_transaction_dts.second),
				   &(pOut->trade_info[i].cash_transaction_dts.fraction));
			pOut->trade_info[i].cash_transaction_dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
		}


		/* SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id
		   FROM trade_history
		   WHERE th_t_id = %s
		   ORDER BY th_dts
		   LIMIT 3 */


		stmt = m_Stmt;
		ostringstream osTLF2_4;

		osTLF2_4 << "SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = " <<
	    pOut->trade_info[i].trade_id << " ORDER BY th_dts LIMIT 3";

		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF2_4.str().c_str()), SQL_NTS);

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 1, SQL_C_CHAR, datetime_buf, 30, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_history_status_id, cTH_ST_ID_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

		int j = 0;
		while(rc != SQL_NO_DATA_FOUND && j < 3)
		{
			//pOut->trade_info[i].trade_history_dts[j] = datetime_buf
			sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
				   &(pOut->trade_info[i].trade_history_dts[j].year),
				   &(pOut->trade_info[i].trade_history_dts[j].month),
				   &(pOut->trade_info[i].trade_history_dts[j].day),
				   &(pOut->trade_info[i].trade_history_dts[j].hour),
				   &(pOut->trade_info[i].trade_history_dts[j].minute),
				   &(pOut->trade_info[i].trade_history_dts[j].second),
				   &(pOut->trade_info[i].trade_history_dts[j].fraction));
			pOut->trade_info[i].trade_history_dts[j].fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					trade_history_status_id, cTH_ST_ID_len+1);
			j++;

			rc = SQLFetch(stmt);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
				ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		}
		SQLCloseCursor(stmt);
	}

	CommitTxn();

	//pOut->status = CBaseTxnErr::SUCCESS;
}

void CTradeLookupDB::DoTradeLookupFrame3(const TTradeLookupFrame3Input *pIn,
										 TTradeLookupFrame3Output *pOut)
{

	SQLHSTMT stmt;

	SQLRETURN rc;

	char start_trade_dts[30]; //pIn->start_trade_dts
	char end_trade_dts[30]; //pIn->end_trade_dts

	TTradeLookupFrame3TradeInfo trade_info;
	unsigned char is_cash; //SQL_C_BIT

	char date_buf[15]; //pOut->trade_info[].settlement_cash_due_date
	char datetime_buf[30]; //pOut->trade_info[].trade_dts pOut->trade_info[].cash_transaction_dts pOut->trade_info[].trade_history_dts[]

	char trade_history_status_id[cTH_ST_ID_len+1];

	int i;

	stmt = m_Stmt;
	rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	BeginTxn();

	/* SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty,
			  DATE_FORMAT(t_dts, '%Y-%m-%d %H:%i:%s.%f'), t_id, t_tt_id
	   FROM trade
	   WHERE t_s_symb = '%s'
		 AND t_dts >= '%s'
		 AND t_dts <= '%s'
	   ORDER BY t_dts ASC
	   LIMIT pIn->max_trades */

	snprintf(start_trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
			 pIn->start_trade_dts.year,
			 pIn->start_trade_dts.month,
			 pIn->start_trade_dts.day,
			 pIn->start_trade_dts.hour,
			 pIn->start_trade_dts.minute,
			 pIn->start_trade_dts.second,
			 pIn->start_trade_dts.fraction / 1000); //nano -> micro
	snprintf(end_trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
			 pIn->end_trade_dts.year,
			 pIn->end_trade_dts.month,
			 pIn->end_trade_dts.day,
			 pIn->end_trade_dts.hour,
			 pIn->end_trade_dts.minute,
			 pIn->end_trade_dts.second,
			 pIn->end_trade_dts.fraction / 1000); //nano -> micro

	stmt = m_Stmt;
	ostringstream osTLF3_1;

	osTLF3_1 << "SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty, DATE_FORMAT(t_dts, '%Y-%m-%d %H:%i:%s.%f'), t_id, t_tt_id FROM trade WHERE t_s_symb = '" <<
	pIn->symbol << "' AND t_dts >= '" <<
	start_trade_dts << "' AND t_dts <= '" <<
	end_trade_dts << "' ORDER BY t_dts LIMIT " <<
	pIn->max_trades;

	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF3_1.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &(trade_info.acct_id), 0, NULL);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_info.exec_name, cEXEC_NAME_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 3, SQL_C_BIT, &is_cash, 0, NULL); //pOut->trade_info[i].is_cash
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 4, SQL_C_DOUBLE, &(trade_info.price), 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 5, SQL_C_LONG, &(trade_info.quantity), 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 6, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->trade_info[].trade_dts
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	rc = SQLBindCol(stmt, 7, SQL_C_SBIGINT, &(trade_info.trade_id), 0, NULL);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 8, SQL_C_CHAR, trade_info.trade_type, cTT_ID_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	i = 0;
	while(rc != SQL_NO_DATA_FOUND && i < pIn->max_trades)
	{
		pOut->trade_info[i].acct_id = trade_info.acct_id;
		strncpy(pOut->trade_info[i].exec_name, trade_info.exec_name, cEXEC_NAME_len+1);
		pOut->trade_info[i].is_cash = (is_cash != 0);
		pOut->trade_info[i].price = trade_info.price;
		pOut->trade_info[i].quantity = trade_info.quantity;

		//pOut->trade_info[i].trade_dts = datetime_buf
		sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
			   &(pOut->trade_info[i].trade_dts.year),
			   &(pOut->trade_info[i].trade_dts.month),
			   &(pOut->trade_info[i].trade_dts.day),
			   &(pOut->trade_info[i].trade_dts.hour),
			   &(pOut->trade_info[i].trade_dts.minute),
			   &(pOut->trade_info[i].trade_dts.second),
			   &(pOut->trade_info[i].trade_dts.fraction));
		pOut->trade_info[i].trade_dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.

		pOut->trade_info[i].trade_id = trade_info.trade_id;
		strncpy(pOut->trade_info[i].trade_type, trade_info.trade_type, cTT_ID_len+1);
		i++;

		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	}
	SQLCloseCursor(stmt);

	pOut->num_found = i;


	for (i = 0; i < pOut->num_found; i++)
	{
		/* SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type
		   FROM settlement
		   WHERE se_t_id = %s */

		stmt = m_Stmt;
		ostringstream osTLF3_2;

		osTLF3_2 << "SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = " <<

		pOut->trade_info[i].trade_id;
		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF3_2.str().c_str()), SQL_NTS);


		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].settlement_amount), 0, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 2, SQL_C_CHAR, date_buf, 15, NULL); //pOut->trade_info[].settlement_cash_due_date
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].settlement_cash_type, cSE_CASH_TYPE_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		SQLCloseCursor(stmt);

		//pOut->trade_info[i].settlement_cash_due_date = date_buf
		sscanf(date_buf,"%4hd-%2hu-%2hu",
			   &(pOut->trade_info[i].settlement_cash_due_date.year),
			   &(pOut->trade_info[i].settlement_cash_due_date.month),
			   &(pOut->trade_info[i].settlement_cash_due_date.day));
		pOut->trade_info[i].settlement_cash_due_date.hour
				= pOut->trade_info[i].settlement_cash_due_date.minute
				= pOut->trade_info[i].settlement_cash_due_date.second
				= 0;
		pOut->trade_info[i].settlement_cash_due_date.fraction = 0;


		if(pOut->trade_info[i].is_cash)
		{
			/* SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name
			   FROM cash_transaction
			   WHERE ct_t_id = %s */

			stmt = m_Stmt;
			ostringstream osTLF3_3;

			osTLF3_3 << "SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
			pOut->trade_info[i].trade_id;
			rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF3_3.str().c_str()), SQL_NTS);


			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].cash_transaction_amount), 0, NULL);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLBindCol(stmt, 2, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->trade_info[].cash_transaction_dts
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].cash_transaction_name, cCT_NAME_len+1, NULL);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			rc = SQLFetch(stmt);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
				ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
			SQLCloseCursor(stmt);

			//pOut->trade_info[i].cash_transaction_dts = datetime_buf
			sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
				   &(pOut->trade_info[i].cash_transaction_dts.year),
				   &(pOut->trade_info[i].cash_transaction_dts.month),
				   &(pOut->trade_info[i].cash_transaction_dts.day),
				   &(pOut->trade_info[i].cash_transaction_dts.hour),
				   &(pOut->trade_info[i].cash_transaction_dts.minute),
				   &(pOut->trade_info[i].cash_transaction_dts.second),
				   &(pOut->trade_info[i].cash_transaction_dts.fraction));
			pOut->trade_info[i].cash_transaction_dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
		}

		/* SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id
		   FROM trade_history
		   WHERE th_t_id = %s
		   ORDER BY th_dts ASC
		   LIMIT 3 */

		stmt = m_Stmt;
		ostringstream osTLF3_4;

		osTLF3_4 << "SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = " <<
	    pOut->trade_info[i].trade_id << " ORDER BY th_dts ASC LIMIT 3";

		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF3_4.str().c_str()), SQL_NTS);


		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 1, SQL_C_CHAR, datetime_buf, 30, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_history_status_id, cTH_ST_ID_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

		int j = 0;
		while(rc != SQL_NO_DATA_FOUND && j < 3)
		{
			//pOut->trade_info[i].trade_history_dts[j] = datetime_buf
			sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
				   &(pOut->trade_info[i].trade_history_dts[j].year),
				   &(pOut->trade_info[i].trade_history_dts[j].month),
				   &(pOut->trade_info[i].trade_history_dts[j].day),
				   &(pOut->trade_info[i].trade_history_dts[j].hour),
				   &(pOut->trade_info[i].trade_history_dts[j].minute),
				   &(pOut->trade_info[i].trade_history_dts[j].second),
				   &(pOut->trade_info[i].trade_history_dts[j].fraction));
			pOut->trade_info[i].trade_history_dts[j].fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					trade_history_status_id, cTH_ST_ID_len+1);
			j++;

			rc = SQLFetch(stmt);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
				ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		}
		SQLCloseCursor(stmt);
	}

	CommitTxn();

	//pOut->status = CBaseTxnErr::SUCCESS;

}

void CTradeLookupDB::DoTradeLookupFrame4(const TTradeLookupFrame4Input *pIn,
										 TTradeLookupFrame4Output *pOut)
{
	int i;
	SQLHSTMT stmt;

	SQLRETURN rc;

	char trade_dts[30]; //pIn->trade_dts

	TTradeLookupFrame4TradeInfo trade_info;

	stmt = m_Stmt;
	rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	BeginTxn();

	/* SELECT t_id
	   FROM trade
	   WHERE t_ca_id = %ld
		 AND t_dts >= '%s'
	   ORDER BY t_dts ASC
	   LIMIT 1 */

	snprintf(trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
			 pIn->trade_dts.year,
			 pIn->trade_dts.month,
			 pIn->trade_dts.day,
			 pIn->trade_dts.hour,
			 pIn->trade_dts.minute,
			 pIn->trade_dts.second,
			 pIn->trade_dts.fraction / 1000); //nano -> micro

	stmt = m_Stmt;
	ostringstream osTLF4_1;
	osTLF4_1 << "SELECT t_id FROM trade WHERE t_ca_id = " <<
	pIn->acct_id << " AND t_dts >= '" <<
	trade_dts << "' ORDER BY t_dts ASC LIMIT 1";

	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF4_1.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &(pOut->trade_id), 0, NULL);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc == SQL_NO_DATA_FOUND) {
		//pOut->status = +641;
		printf("Error!");
		//goto end;
	} else {
		//pOut->status = CBaseTxnErr::SUCCESS;
	}
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	/* SELECT hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty
	   FROM holding_history
	   WHERE hh_h_t_id IN (
			 SELECT hh_h_t_id
			 FROM holding_history
			 WHERE hh_t_id = %s)
	   LIMIT 20 */
	//OR
	/* SELECT h1.hh_h_t_id, h1.hh_t_id, h1.hh_before_qty, h1.hh_after_qty
	   FROM holding_history h1, holding_history h2
	   WHERE h1.hh_h_t_id = h2.hh_h_t_id
	   AND h2.hh_t_id = %s
	   LIMIT 20 */

	stmt = m_Stmt;
	ostringstream osTLF4_2;
	osTLF4_2 << "SELECT h1.hh_h_t_id, h1.hh_t_id, h1.hh_before_qty, h1.hh_after_qty FROM holding_history h1, holding_history h2 WHERE h1.hh_h_t_id = h2.hh_h_t_id AND h2.hh_t_id = " <<
	pOut->trade_id << " LIMIT 20";

	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTLF4_2.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &(trade_info.holding_history_id), 0, NULL);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	rc = SQLBindCol(stmt, 2, SQL_C_SBIGINT, &(trade_info.holding_history_trade_id), 0, NULL);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 3, SQL_C_LONG, &(trade_info.quantity_before), 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 4, SQL_C_LONG, &(trade_info.quantity_after), 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	i = 0;
	while(rc != SQL_NO_DATA_FOUND && i < 20)
	{
		pOut->trade_info[i].holding_history_id = trade_info.holding_history_id;
		pOut->trade_info[i].holding_history_trade_id = trade_info.holding_history_trade_id;
		pOut->trade_info[i].quantity_before = trade_info.quantity_before;
		pOut->trade_info[i].quantity_after = trade_info.quantity_after;
		i++;

		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	}
	SQLCloseCursor(stmt);

	pOut->num_found = i;

	end:
	CommitTxn();
}
