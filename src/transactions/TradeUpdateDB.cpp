
#include "TradeUpdateDB.h"

using namespace TPCE;

CTradeUpdateDB::CTradeUpdateDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CTradeUpdateDB::~CTradeUpdateDB()
{
}

void CTradeUpdateDB::DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn,
					 TTradeUpdateFrame1Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    char old_ex_name[cEXEC_NAME_len+1];
    char new_ex_name[cEXEC_NAME_len+1];

    unsigned char is_cash; //SQL_C_BIT
    unsigned char is_market; //SQL_C_BIT

    char date_buf[15]; //pOut->trade_info[].settlement_cash_due_date
    char datetime_buf[30]; //pOut->trade_info[].cash_transaction_dts pOut->trade_info[].trade_history_dts[]
    char trade_history_status_id[cTH_ST_ID_len+1];

    stmt = m_Stmt;

    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();


    pOut->num_found = 0;
    pOut->num_updated = 0;

    for (int i = 0; i < pIn->max_trades; i++)
    {
	if (pOut->num_updated < pIn->max_updates)
	{
	    /* SELECT t_exec_name
	       FROM trade
	       WHERE t_id = %ld */

	    stmt = m_Stmt;
	    ostringstream osTUF1_1;
	    osTUF1_1 << "SELECT t_exec_name FROM trade WHERE t_id = " <<
		pIn->trade_id[i];
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_1.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, old_ex_name, cEXEC_NAME_len+1, NULL);
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

	    char *p_index;
	    if ((p_index = strstr(old_ex_name, " X ")) != 0)
	    {
		strncpy(new_ex_name, old_ex_name, p_index - old_ex_name);
		*(new_ex_name + (p_index - old_ex_name)) = ' ';
		strncpy(new_ex_name + (p_index - old_ex_name) + 1,
			p_index + 3, cEXEC_NAME_len+1 - ((p_index - old_ex_name) + 1));
	    }
	    else if ((p_index = index(old_ex_name,(int)' ')) != 0)
	    {
		strncpy(new_ex_name, old_ex_name, p_index - old_ex_name);
		strcpy(new_ex_name + (p_index - old_ex_name), " X ");
		strncpy(new_ex_name + (p_index - old_ex_name) + 3,
			p_index + 1, cEXEC_NAME_len+1 - ((p_index - old_ex_name) + 3));
	    }
	    else
	    {
		strncpy(new_ex_name, old_ex_name, cEXEC_NAME_len+1);
	    }


	    /* UPDATE trade
	       SET t_exec_name = '%s'
	       WHERE t_id = %ld */

	    stmt = m_Stmt;
	    ostringstream osTUF1_2;
	    osTUF1_2 << "UPDATE trade SET t_exec_name = '" <<
		new_ex_name << "' WHERE t_id = " <<
		pIn->trade_id[i];
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_2.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    
	    SQLLEN row_count;
	    SQLRowCount(stmt, &row_count);

	    SQLCloseCursor(stmt);

	    pOut->num_updated += row_count;
	}


	/* SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt, t_trade_price
	   FROM trade, trade_type
	   WHERE t_id = %ld
	     AND t_tt_id = tt_id */

	stmt = m_Stmt;
	ostringstream osTUF1_3;
	osTUF1_3 << "SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt, t_trade_price FROM trade, trade_type WHERE t_id = " <<
            pIn->trade_id[i] << " AND t_tt_id = tt_id";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_3.str().c_str()), SQL_NTS);


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
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        SQLCloseCursor(stmt);

        pOut->trade_info[i].is_cash = (is_cash != 0);
        pOut->trade_info[i].is_market = (is_market != 0);


	/* SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type
	   FROM settlement
	   WHERE se_t_id = %ld */

	stmt = m_Stmt;
	ostringstream osTUF1_4;

	osTUF1_4 << "SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = " <<
            pIn->trade_id[i];
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_4.str().c_str()), SQL_NTS);


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
	    ostringstream osTUF1_5;

	    osTUF1_5 << "SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
                pIn->trade_id[i];
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_5.str().c_str()), SQL_NTS);


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
	ostringstream osTUF1_6;
	osTUF1_6 << "SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pIn->trade_id[i] << " ORDER BY th_dts LIMIT 3";

	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_6.str().c_str()), SQL_NTS);


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

void CTradeUpdateDB::DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn,
					 TTradeUpdateFrame2Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    char cash_type[cSE_CASH_TYPE_len+1];

    char start_trade_dts[30]; //pIn->start_trade_dts
    char end_trade_dts[30]; //pIn->end_trade_dts

    TTradeUpdateFrame2TradeInfo trade_info;
    unsigned char is_cash; //SQL_C_BIT

    char date_buf[15]; //pOut->trade_info[].settlement_cash_due_date
    char datetime_buf[30]; //pOut->trade_info[].cash_transaction_dts pOut->trade_info[].trade_history_dts[]

    char trade_history_status_id[cTH_ST_ID_len+1];

    int i;
    stmt = m_Stmt;

    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();

    /* SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price
       FROM trade
       WHERE t_ca_id = %ld
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
    ostringstream osTUF2_1;
    osTUF2_1 << "SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price FROM trade WHERE t_ca_id = " <<
        pIn->acct_id << " AND t_dts >= '" <<
        start_trade_dts << "' AND t_dts <= '" <<
        end_trade_dts << "' ORDER BY t_dts ASC LIMIT " <<
        pIn->max_trades;

    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_1.str().c_str()), SQL_NTS);


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
    pOut->num_updated = 0;


    for (i = 0; i < pOut->num_found; i++)
    {
	if (pOut->num_updated < pIn->max_updates)
	{
	    /* SELECT se_cash_type
	       FROM settlement
	       WHERE se_t_id = %s */

	    stmt = m_Stmt;
	    ostringstream osTUF2_2;
	    osTUF2_2 << "SELECT se_cash_type FROM settlement WHERE se_t_id = " <<
		pOut->trade_info[i].trade_id;
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_2.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, cash_type, cSE_CASH_TYPE_len+1, NULL);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLFetch(stmt);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);


	    if(pOut->trade_info[i].is_cash)
	    {
		if (strcmp(cash_type, "Cash Account") == 0)
		    strcpy(cash_type, "Cash");
		else
		    strcpy(cash_type, "Cash Account");
	    }
	    else
	    {
		if (strcmp(cash_type, "Margin Account") == 0)
		    strcpy(cash_type, "Margin");
		else
		    strcpy(cash_type, "Margin Account");
	    }

	    /* UPDATE settlement
	       SET se_cash_type = '%s'
	       WHERE se_t_id = %s */

	    stmt = m_Stmt;
	    ostringstream osTUF2_3;
	    osTUF2_3 << "UPDATE settlement SET se_cash_type = '" <<
		cash_type << "' WHERE se_t_id = " <<
		pOut->trade_info[i].trade_id;
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_3.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	    SQLLEN row_count;
	    SQLRowCount(stmt, &row_count);

	    SQLCloseCursor(stmt);

	    pOut->num_updated += row_count;
	}

	/* SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type
	   FROM settlement
	   WHERE se_t_id = %s */

	stmt = m_Stmt;
	ostringstream osTUF2_4;

	osTUF2_4 << "SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = " <<
            pOut->trade_info[i].trade_id;
        rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_4.str().c_str()), SQL_NTS);


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
	    ostringstream osTUF2_5;

	    osTUF2_5 << "SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
                pOut->trade_info[i].trade_id;
            rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_5.str().c_str()), SQL_NTS);


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
	ostringstream osTUF2_6;

	osTUF2_6 << "SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pOut->trade_info[i].trade_id << " ORDER BY th_dts LIMIT 3";

        rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_6.str().c_str()), SQL_NTS);


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

void CTradeUpdateDB::DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn,
					 TTradeUpdateFrame3Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    char ct_name[cCT_NAME_len+1];

    char start_trade_dts[30]; //pIn->start_trade_dts
    char end_trade_dts[30]; //pIn->end_trade_dts

    TTradeUpdateFrame3TradeInfo trade_info;
    unsigned char is_cash; //SQL_C_BIT

    char date_buf[15]; //pOut->trade_info[].settlement_cash_due_date
    char datetime_buf[30]; //pOut->trade_info[].trade_dts pOut->trade_info[].cash_transaction_dts pOut->trade_info[].trade_history_dts[]

    char trade_history_status_id[cTH_ST_ID_len+1];

    int i;



    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();

    /* SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty,
              s_name, DATE_FORMAT(t_dts, '%Y-%m-%d %H:%i:%s.%f'), t_id, t_tt_id, tt_name
       FROM trade, trade_type, security
       WHERE t_s_symb = '%s'
         AND t_dts >= '%s'
         AND t_dts <= '%s'
         AND tt_id = t_tt_id
         AND s_symb = t_s_symb
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
    ostringstream osTUF3_1;

    osTUF3_1 << "SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty, s_name, DATE_FORMAT(t_dts, '%Y-%m-%d %H:%i:%s.%f'), t_id, t_tt_id, tt_name FROM trade, trade_type FORCE INDEX(PRIMARY), security WHERE t_s_symb = '" <<
	pIn->symbol << "' AND t_dts >= '" <<
	start_trade_dts << "' AND t_dts <= '" <<
	end_trade_dts << "' AND tt_id = t_tt_id AND s_symb = t_s_symb ORDER BY t_dts ASC LIMIT " <<
	pIn->max_trades;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_1.str().c_str()), SQL_NTS);


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
    rc = SQLBindCol(stmt, 6, SQL_C_CHAR, trade_info.s_name, cS_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 7, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->trade_info[].trade_dts
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 8, SQL_C_SBIGINT, &(trade_info.trade_id), 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 9, SQL_C_CHAR, trade_info.trade_type, cTT_ID_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 10, SQL_C_CHAR, trade_info.type_name, cTT_NAME_len+1, NULL);
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
	strncpy(pOut->trade_info[i].s_name, trade_info.s_name, cS_NAME_len+1);

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
	strncpy(pOut->trade_info[i].type_name, trade_info.type_name, cTT_NAME_len+1);
	i++;

        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    pOut->num_found = i;
    pOut->num_updated = 0;


    for (i = 0; i < pOut->num_found; i++)
    {
	/* SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type
	   FROM settlement
	   WHERE se_t_id = %s" */

	stmt = m_Stmt;
	ostringstream osTUF3_2;

	osTUF3_2 << "SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = " <<
            pOut->trade_info[i].trade_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_2.str().c_str()), SQL_NTS);


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
	    if (pOut->num_updated < pIn->max_updates)
	    {
		/* SELECT ct_name
		   FROM cash_transaction
		   WHERE ct_t_id = %s */

		stmt = m_Stmt;
		ostringstream osTUF3_3;
		osTUF3_3 << "SELECT ct_name FROM cash_transaction WHERE ct_t_id = " <<
		    pOut->trade_info[i].trade_id;
		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_3.str().c_str()), SQL_NTS);


		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 1, SQL_C_CHAR, ct_name, cCT_NAME_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
		    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		SQLCloseCursor(stmt);


		/* UPDATE cash_transaction
		   SET ct_name = '%s'
		   WHERE ct_t_id = %s */

		char s_name[cS_NAME_len+1];
		expand_quote(s_name, pOut->trade_info[i].s_name, cS_NAME_len+1);

		stmt = m_Stmt;
		ostringstream osTUF3_4;
		osTUF3_4 << "UPDATE cash_transaction SET ct_name = '" <<
		    pOut->trade_info[i].type_name << " " << pOut->trade_info[i].quantity;
		if(strstr(ct_name, " shares of "))
		    osTUF3_4 << " Shares of ";
		else
		    osTUF3_4 << " shares of ";
		osTUF3_4 << s_name << "' WHERE ct_t_id = " <<
		    pOut->trade_info[i].trade_id;

		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_4.str().c_str()), SQL_NTS);


		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

		SQLLEN row_count;
		SQLRowCount(stmt, &row_count);

		SQLCloseCursor(stmt);

		pOut->num_updated += row_count;
	    }

	    /* SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name
	       FROM cash_transaction
	       WHERE ct_t_id = %s */

	    stmt = m_Stmt;
	    ostringstream osTUF3_5;

	    osTUF3_5 << "SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
                pOut->trade_info[i].trade_id;
            rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_5.str().c_str()), SQL_NTS);


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
	ostringstream osTUF3_6;
	osTUF3_6 << "SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pOut->trade_info[i].trade_id << " ORDER BY th_dts ASC LIMIT 3";
        rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_6.str().c_str()), SQL_NTS);


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
