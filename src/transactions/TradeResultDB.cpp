#include "TradeResultDB.h"

using namespace TPCE;

CTradeResultDB::CTradeResultDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CTradeResultDB::~CTradeResultDB()
{
}

void CTradeResultDB::DoTradeResultFrame1(
    const TTradeResultFrame1Input *pIn,
    TTradeResultFrame1Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();


    /* SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg, t_lifo, t_is_cash
       FROM trade
       WHERE t_id = %d */

    stmt = m_Stmt;
    ostringstream osTRF1_1;
    osTRF1_1 << "SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg, t_lifo, t_is_cash FROM trade WHERE t_id = " <<
	pIn->trade_id;

    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF1_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &(pOut->acct_id), 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, pOut->type_id, cTT_ID_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->symbol, cSYMBOL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_LONG, &(pOut->trade_qty), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_DOUBLE, &(pOut->charge), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 6, SQL_C_LONG, &(pOut->is_lifo), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 7, SQL_C_LONG, &(pOut->trade_is_cash), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc == SQL_NO_DATA_FOUND)
	//pOut->status = -811; /* Anyway, SQL_NO_DATA_FOUND cannot continue Trade-Result... */
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    /* SELECT tt_name, tt_is_sell, tt_is_mrkt
       FROM trade_type
       WHERE tt_id = '%s' */

    stmt = m_Stmt;
    ostringstream osTRF1_2;
    osTRF1_2 << "SELECT tt_name, tt_is_sell, tt_is_mrkt FROM trade_type WHERE tt_id = '" <<
	pOut->type_id << "'";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF1_2.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->type_name, cTT_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_LONG, &(pOut->type_is_sell), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_LONG, &(pOut->type_is_market), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /* SELECT hs_qty
       FROM holding_summary
       WHERE hs_ca_id = %s
         AND hs_s_symb = '%s' */

    stmt = m_Stmt;
    ostringstream osTRF1_3;
    osTRF1_3 << "SELECT hs_qty FROM holding_summary WHERE hs_ca_id = " <<
	pOut->acct_id << " AND hs_s_symb = '" <<
	pOut->symbol << "'";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF1_3.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_LONG, &(pOut->hs_qty), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    if (rc == SQL_NO_DATA_FOUND) //=== SQL_NO_DATA_FOUND is allowed.
    {
	pOut->hs_qty = 0;
    }

    //pOut->status = CBaseTxnErr::SUCCESS;
    pOut->num_found = 1;
}

void CTradeResultDB::DoTradeResultFrame2(
    const TTradeResultFrame2Input *pIn,
    TTradeResultFrame2Output *pOut)
{
    SQLHSTMT stmt;
    SQLHSTMT stmt2;

    SQLRETURN rc;

    char now_dts[TIMESTAMP_LEN+1];

    TIdent hold_id;
    double hold_price;
    INT32 hold_qty;
    INT32 needed_qty;

    gettimestamp(now_dts, STRFTIME_FORMAT, TIMESTAMP_LEN);
    sscanf(now_dts, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu",
	   &(pOut->trade_dts.year),
	   &(pOut->trade_dts.month),
	   &(pOut->trade_dts.day),
	   &(pOut->trade_dts.hour),
	   &(pOut->trade_dts.minute),
	   &(pOut->trade_dts.second));
    pOut->trade_dts.fraction = 0;

    pOut->buy_value = 0.0;
    pOut->sell_value = 0.0;
    needed_qty = pIn->trade_qty;

    /* SELECT ca_b_id, ca_c_id, ca_tax_st
       FROM customer_account
       WHERE ca_id = %d */

    stmt = m_Stmt;
    ostringstream osTRF2_1;
    osTRF2_1 << "SELECT ca_b_id, ca_c_id, ca_tax_st FROM customer_account WHERE ca_id = " <<
	pIn->acct_id;

    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &(pOut->broker_id), 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 2, SQL_C_SBIGINT, &(pOut->cust_id), 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_LONG, &(pOut->tax_status), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    if (pIn->type_is_sell)
    {
	if (pIn->hs_qty == 0)
	{
	    /* INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty)
	       VALUES(%d, '%s', %d) */
	    stmt = m_Stmt;
	    ostringstream osTRF2_2a;
	    osTRF2_2a << "INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty) VALUES(" <<
		pIn->acct_id << ", '" <<
		pIn->symbol << "', " <<
		(-(pIn->trade_qty)) << ")";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_2a.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);
	}
	else if (pIn->hs_qty != pIn->trade_qty)
	{
	    /* UPDATE holding_summary
	       SET hs_qty = %d
	       WHERE hs_ca_id = %d
	         AND hs_s_symb = '%s' */

	    stmt = m_Stmt;
	    ostringstream osTRF2_2b;
	    osTRF2_2b << "UPDATE holding_summary SET hs_qty = " <<
		(pIn->hs_qty - pIn->trade_qty) << " WHERE hs_ca_id = " <<
		pIn->acct_id << " AND hs_s_symb = '" <<
		pIn->symbol << "'";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_2b.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);
	}

	if (pIn->hs_qty > 0)
	{
	    /* SELECT h_t_id, h_qty, h_price
	       FROM holding
	       WHERE h_ca_id = %d
	         AND h_s_symb = '%s'
	       ORDER BY h_dts [DESC|ASC] */

	    stmt2 = m_Stmt2;
	    ostringstream osTRF2_3;
	    osTRF2_3 << "SELECT h_t_id, h_qty, h_price FROM holding WHERE h_ca_id = " <<

		pIn->acct_id << " AND h_s_symb = '" <<
		pIn->symbol << "' ORDER BY h_dts ";
            if ( pIn->is_lifo )
                osTRF2_3 << "DESC";
            else
                osTRF2_3 << "ASC";
	    rc = SQLExecDirect(stmt2, (SQLCHAR*)(osTRF2_3.str().c_str()), SQL_NTS);


            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

            rc = SQLBindCol(stmt2, 1, SQL_C_SBIGINT, &hold_id, 0, NULL);

            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt2, 2, SQL_C_LONG, &hold_qty, 0, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt2, 3, SQL_C_DOUBLE, &hold_price, 0, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

	    rc = SQLFetch(stmt2);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


	    while( needed_qty != 0 && rc != SQL_NO_DATA_FOUND )
	    {
		try
		{

		if (hold_qty > needed_qty)
		{
		    /* INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty,
		                                   hh_after_qty)
		       VALUES(%d, %d, %d, %d) */

		    stmt = m_Stmt;
		    ostringstream osTRF2_4;
		    osTRF2_4 << "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) VALUES(" <<
			hold_id << ", " <<
			pIn->trade_id << ", " <<
			hold_qty << ", " <<
			(hold_qty - needed_qty) << ")";
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_4.str().c_str()), SQL_NTS);


		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);

		    /* UPDATE holding
		       SET h_qty = %d
		       WHERE h_t_id = %d */

		    stmt = m_Stmt;
		    ostringstream osTRF2_5a;
		    osTRF2_5a << "UPDATE holding SET h_qty = " <<
			(hold_qty - needed_qty) << " WHERE h_t_id = " <<
			hold_id;
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_5a.str().c_str()), SQL_NTS);


		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    pOut->buy_value += (double)needed_qty * hold_price;
		    pOut->sell_value += (double)needed_qty * pIn->trade_price;
		    needed_qty = 0;
		}
		else
		{
		    /* INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty,
		                                   hh_after_qty)
		       VALUES(%d, %d, %d, %d) */

		    stmt = m_Stmt;
		    ostringstream osTRF2_4;
		    osTRF2_4 << "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) VALUES(" <<
			hold_id << ", " <<
			pIn->trade_id << ", " <<
			hold_qty << ", 0)";
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_4.str().c_str()), SQL_NTS);


		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    /* DELETE FROM holding
		       WHERE h_t_id = %d */

		    stmt = m_Stmt;
		    ostringstream osTRF2_5b;
		    osTRF2_5b << "DELETE FROM holding WHERE h_t_id = " <<
			hold_id;
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_5b.str().c_str()), SQL_NTS);


		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    pOut->buy_value += (double)hold_qty * hold_price;
		    pOut->sell_value += (double)hold_qty * pIn->trade_price;
		    needed_qty = needed_qty - hold_qty;
		}

		}
		catch (const CODBCERR* e)
		{
		    SQLCloseCursor(stmt2);
		    throw;
		}

		rc = SQLFetch(stmt2);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
	    }
	    SQLCloseCursor(stmt2);
	}

	if (needed_qty > 0)
	{
	    /* INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty,
	                                   hh_after_qty)
	       VALUES(%d, %d, %d, %d) */
	    stmt = m_Stmt;
	    ostringstream osTRF2_6;
	    osTRF2_6 << "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) VALUES(" <<
		pIn->trade_id << ", " <<
		pIn->trade_id << ", 0, " <<
		(-needed_qty) << ")";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_6.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);


	    /* INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price,
	                           h_qty)
	       VALUES (%d, %d, '%s', '%s', %f, %d) */
	    stmt = m_Stmt;
	    ostringstream osTRF2_7a;
	    osTRF2_7a << "INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, h_qty) VALUES(" <<
		pIn->trade_id << ", " <<
		pIn->acct_id << ", '" <<
		pIn->symbol << "', '" <<
		now_dts << "', " <<
		pIn->trade_price << ", " <<
		(-needed_qty) << ")";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_7a.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);
	}
	else if (pIn->hs_qty == pIn->trade_qty)
	{
	    /* DELETE FROM holding_summary
	       WHERE hs_ca_id = %d
	         AND hs_s_symb = '%s' */

	    stmt = m_Stmt;
	    ostringstream osTRF2_7b;
	    osTRF2_7b << "DELETE FROM holding_summary WHERE hs_ca_id = " <<
		pIn->acct_id << " AND hs_s_symb = '" <<
		pIn->symbol << "'";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_7b.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);
	}
    }
    else //!pIn->type_is_sell
    {
	if (pIn->hs_qty == 0)
	{
	    /* INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty)
	       VALUES (%d, '%s', %d) */
	    stmt = m_Stmt;
	    ostringstream osTRF2_2a;
	    osTRF2_2a << "INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty) VALUES(" <<
		pIn->acct_id << ", '" <<
		pIn->symbol << "', " <<
		pIn->trade_qty << ")";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_2a.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);
	}
	else if (-(pIn->hs_qty) != pIn->trade_qty)
	{
	    /* UPDATE holding_summary
	       SET hs_qty = %d
	       WHERE hs_ca_id = %d
	         AND hs_s_symb = '%s' */
	    stmt = m_Stmt;
	    ostringstream osTRF2_2b;
	    osTRF2_2b << "UPDATE holding_summary SET hs_qty = " <<
		(pIn->hs_qty + pIn->trade_qty) << " WHERE hs_ca_id = " <<
		pIn->acct_id << " AND hs_s_symb = '" <<
		pIn->symbol << "'";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_2b.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);
	}

	if (pIn->hs_qty < 0)
	{
	    /* SELECT h_t_id, h_qty, h_price
	       FROM holding
	       WHERE h_ca_id = %d
	         AND h_s_symb = '%s'
	       ORDER BY h_dts [DESC|ASC] */

	    stmt2 = m_Stmt2;
	    ostringstream osTRF2_3;

	    osTRF2_3 << "SELECT h_t_id, h_qty, h_price FROM holding WHERE h_ca_id = " <<

		pIn->acct_id << " AND h_s_symb = '" <<
		pIn->symbol << "' ORDER BY h_dts ";
            if ( pIn->is_lifo )
                osTRF2_3 << "DESC";
            else
                osTRF2_3 << "ASC";
	    rc = SQLExecDirect(stmt2, (SQLCHAR*)(osTRF2_3.str().c_str()), SQL_NTS);


            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

            rc = SQLBindCol(stmt2, 1, SQL_C_SBIGINT, &hold_id, 0, NULL);

            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt2, 2, SQL_C_LONG, &hold_qty, 0, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt2, 3, SQL_C_DOUBLE, &hold_price, 0, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

	    rc = SQLFetch(stmt2);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


	    while( needed_qty != 0 && rc != SQL_NO_DATA_FOUND )
	    {
		try
		{

		if (hold_qty + needed_qty < 0)
		{
		    /* INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty,
		                                   hh_after_qty)
		       VALUES(%d, %d, %d, %d) */

		    stmt = m_Stmt;
		    ostringstream osTRF2_4;
		    osTRF2_4 << "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) VALUES(" <<
			hold_id << ", " <<
			pIn->trade_id << ", " <<
			hold_qty << ", " <<
			(hold_qty + needed_qty) << ")";
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_4.str().c_str()), SQL_NTS);


		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    /* UPDATE holding
		       SET h_qty = %d
		       WHERE h_t_id = %d */
		    stmt = m_Stmt;
		    ostringstream osTRF2_5a;
		    osTRF2_5a << "UPDATE holding SET h_qty = " <<
			(hold_qty + needed_qty) << " WHERE h_t_id = " <<
			hold_id;
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_5a.str().c_str()), SQL_NTS);


		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    pOut->sell_value += (double)needed_qty * hold_price;
		    pOut->buy_value += (double)needed_qty * pIn->trade_price;
		    needed_qty = 0;
		}
		else
		{
		    /* INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty,
		                                   hh_after_qty)
		       VALUES(%d, %d, %d, %d) */

		    stmt = m_Stmt;
		    ostringstream osTRF2_4;
		    osTRF2_4 << "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) VALUES(" <<
			hold_id << ", " <<
			pIn->trade_id << ", " <<
			hold_qty << ", 0)";
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_4.str().c_str()), SQL_NTS);


		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    /* DELETE FROM holding
		       WHERE h_t_id = %d */

		    stmt = m_Stmt;
		    ostringstream osTRF2_5b;
		    osTRF2_5b << "DELETE FROM holding WHERE h_t_id = " <<
			hold_id;
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_5b.str().c_str()), SQL_NTS);


		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    hold_qty = -hold_qty;
		    pOut->sell_value += (double)hold_qty * hold_price;
		    pOut->buy_value += (double)hold_qty * pIn->trade_price;
		    needed_qty = needed_qty - hold_qty;
		}

		}
		catch (const CODBCERR* e)
		{
		    SQLCloseCursor(stmt2);
		    throw;
		}

		rc = SQLFetch(stmt2);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
	    }
	    SQLCloseCursor(stmt2);
	}

	if (needed_qty > 0)
	{
	    /* INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty,
	                                   hh_after_qty)
	       VALUES(%d, %d, %d, %d) */

	    stmt = m_Stmt;
	    ostringstream osTRF2_6;
	    osTRF2_6 << "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) VALUES(" <<
		pIn->trade_id << ", " <<
		pIn->trade_id << ", 0, " <<
		needed_qty << ")";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_6.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);


	    /* INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price,
	                           h_qty
	       VALUES (%d, %d, '%s', '%s', %f, %d) */
	    stmt = m_Stmt;
	    ostringstream osTRF2_7a;
	    osTRF2_7a << "INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, h_qty) VALUES(" <<
		pIn->trade_id << ", " <<
		pIn->acct_id << ", '" <<
		pIn->symbol << "', '" <<
		now_dts << "', " <<
		pIn->trade_price << ", " <<
		needed_qty << ")";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_7a.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);
	}
	else if (-(pIn->hs_qty) == pIn->trade_qty)
	{
	    /* DELETE FROM holding_summary
	       WHERE hs_ca_id = %d
	         AND hs_s_symb = '%s' */

	    stmt = m_Stmt;
	    ostringstream osTRF2_7b;
	    osTRF2_7b << "DELETE FROM holding_summary WHERE hs_ca_id = " <<
		pIn->acct_id << " AND hs_s_symb = '" <<
		pIn->symbol << "'";
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF2_7b.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);
	}
    }

    //pOut->status = CBaseTxnErr::SUCCESS;
}

void CTradeResultDB::DoTradeResultFrame3(
    const TTradeResultFrame3Input *pIn,
    TTradeResultFrame3Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    double tax_rates;

    /* SELECT SUM(tx_rate)
       FROM taxrate
       WHERE tx_id in (SELECT cx_tx_id
                       FROM customer_taxrate
                       WHERE cx_c_id = %d) */
    //OR
    /* SELECT SUM(tx_rate)
       FROM taxrate, customer_taxrate
       WHERE tx_id = cx_tx_id
         AND cx_c_id = %d */
    stmt = m_Stmt;
    ostringstream osTRF3_1;
    osTRF3_1 << "SELECT sum(tx_rate) FROM taxrate, customer_taxrate WHERE tx_id = cx_tx_id AND cx_c_id = " <<
	pIn->cust_id;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF3_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &tax_rates, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    pOut->tax_amount = (pIn->sell_value - pIn->buy_value) * tax_rates;


    /* UPDATE trade
       SET t_tax = %s
       WHERE t_id = %d */


    stmt = m_Stmt;
    ostringstream osTRF3_2;
    osTRF3_2 << "UPDATE trade SET t_tax = " <<
	pOut->tax_amount << " WHERE t_id = " <<
	pIn->trade_id;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF3_2.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    //pOut->status = CBaseTxnErr::SUCCESS;

}

void CTradeResultDB::DoTradeResultFrame4(
    const TTradeResultFrame4Input *pIn,
    TTradeResultFrame4Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    char s_ex_id[cEX_ID_len+1];
    INT32 c_tier;

    /* SELECT s_ex_id, s_name
       FROM security
       WHERE s_symb = '%s' */

    stmt = m_Stmt;
    ostringstream osTRF4_1;
    osTRF4_1 << "SELECT s_ex_id, s_name FROM security WHERE s_symb = '" <<
	pIn->symbol << "'";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF4_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, s_ex_id, cEX_ID_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, pOut->s_name, cS_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /* SELECT c_tier
       FROM customer
       WHERE c_id = %d */

    stmt = m_Stmt;
    ostringstream osTRF4_2;
    osTRF4_2 << "SELECT c_tier FROM customer WHERE c_id = " <<
	pIn->cust_id;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF4_2.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_LONG, &c_tier, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /* SELECT cr_rate
       FROM commission_rate
       WHERE cr_c_tier = %s
         AND cr_tt_id = '%s'
         AND cr_ex_id = '%s'
         AND cr_from_qty <= %d
         AND cr_to_qty >= %d */

    stmt = m_Stmt;
    ostringstream osTRF4_3;
    osTRF4_3 << "SELECT cr_rate FROM commission_rate WHERE cr_c_tier = " <<
	c_tier << " AND cr_tt_id = '" <<
	pIn->type_id << "' AND cr_ex_id = '" <<
	s_ex_id << "' AND cr_from_qty <= " <<
	pIn->trade_qty << " AND cr_to_qty >= " <<
	pIn->trade_qty;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF4_3.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->comm_rate), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    //pOut->status = CBaseTxnErr::SUCCESS;

}

void CTradeResultDB::DoTradeResultFrame5(
    const TTradeResultFrame5Input *pIn/*,
    TTradeResultFrame5Output *pOut*/)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    char trade_dts[30];//pIn->trade_dts


    snprintf(trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
             pIn->trade_dts.year,
             pIn->trade_dts.month,
             pIn->trade_dts.day,
             pIn->trade_dts.hour,
             pIn->trade_dts.minute,
             pIn->trade_dts.second,
             pIn->trade_dts.fraction / 1000); //nano -> micro


    /* UPDATE trade
       SET t_comm = %f,
           t_dts = '%s',
           t_st_id = '%s',
           t_trade_price = %f
       WHERE t_id = %d */

    stmt = m_Stmt;
    ostringstream osTRF5_1;
    osTRF5_1 << "UPDATE trade SET t_comm = " <<
	pIn->comm_amount << ", t_dts = '" <<
	trade_dts << "', t_st_id = '" <<
	pIn->st_completed_id << "', t_trade_price = " <<
	pIn->trade_price << " WHERE t_id = " <<
	pIn->trade_id;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF5_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /* INSERT INTO trade_history(th_t_id, th_dts, th_st_id)
       VALUES (%d, '%s', '%s') */

    stmt = m_Stmt;
    ostringstream osTRF5_2;
    osTRF5_2 << "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) VALUES(" <<
	pIn->trade_id << ", '" <<
	trade_dts << "', '" <<
	pIn->st_completed_id << "')";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF5_2.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /* UPDATE broker
       SET b_comm_total = b_comm_total + %f,
           b_num_trades = b_num_trades + 1
       WHERE b_id = %d */

    stmt = m_Stmt;
    ostringstream osTRF5_3;
    osTRF5_3 << "UPDATE broker SET b_comm_total = b_comm_total + " <<
	pIn->comm_amount << ", b_num_trades = b_num_trades + 1 WHERE b_id = " <<
	pIn->broker_id;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF5_3.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


   // pOut->status = CBaseTxnErr::SUCCESS;

}

void CTradeResultDB::DoTradeResultFrame6(
    const TTradeResultFrame6Input *pIn,
    TTradeResultFrame6Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    char due_date[15];//pIn->due_date
    char trade_dts[30];//pIn->trade_dts

    char cash_type[cSE_CASH_TYPE_len+1];

    snprintf(due_date, 15, "%04hd-%02hu-%02hu",
	     pIn->due_date.year,
	     pIn->due_date.month,
	     pIn->due_date.day);

    snprintf(trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
             pIn->trade_dts.year,
             pIn->trade_dts.month,
             pIn->trade_dts.day,
             pIn->trade_dts.hour,
             pIn->trade_dts.minute,
             pIn->trade_dts.second,
             pIn->trade_dts.fraction / 1000); //nano -> micro

    if ( pIn->trade_is_cash )
	strncpy(cash_type, "Cash Account", cSE_CASH_TYPE_len+1);
    else
	strncpy(cash_type, "Margin", cSE_CASH_TYPE_len+1);


    /* INSERT INTO settlement(se_t_id, se_cash_type, se_cash_due_date, se_amt)
       VALUES (%d, '%s', '%s', %f) */

    stmt = m_Stmt;
    ostringstream osTRF6_1;
    osTRF6_1 << "INSERT INTO settlement(se_t_id, se_cash_type, se_cash_due_date, se_amt) VALUES(" <<
	pIn->trade_id << ", '" <<
	cash_type << "', '" <<
	due_date << "', " <<
	pIn->se_amount << ")";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF6_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    if ( pIn->trade_is_cash )
    {
	/* UPDATE customer_account
	   SET ca_bal = ca_bal + %f
	   WHERE ca_id = %d */

	stmt = m_Stmt;
	ostringstream osTRF6_2;
	osTRF6_2 << "UPDATE customer_account SET ca_bal = ca_bal + " <<
	    pIn->se_amount << " WHERE ca_id = " <<
	    pIn->acct_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF6_2.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);


	/* INSERT INTO cash_transaction(ct_dts, ct_t_id, ct_amt, ct_name)
	   VALUES ('%s', %d, %f, '%s %d shared of %s') */

	char s_name[cS_NAME_len+1];
	expand_quote(s_name, pIn->s_name, cS_NAME_len+1);

	stmt = m_Stmt;
	ostringstream osTRF6_3;
	osTRF6_3 << "INSERT INTO cash_transaction(ct_dts, ct_t_id, ct_amt, ct_name) VALUES('" <<
	    trade_dts << "', " <<
	    pIn->trade_id << ", " <<
	    pIn->se_amount << ", '" <<
	    pIn->type_name << " " << pIn->trade_qty << " shared of " << s_name << "')";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF6_3.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);
    }

    /* SELECT ca_bal
       FROM customer_account
       WHERE ca_id = %d */

    stmt = m_Stmt;
    ostringstream osTRF6_4;
    osTRF6_4 << "SELECT ca_bal FROM customer_account WHERE ca_id = " <<
	pIn->acct_id;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTRF6_4.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->acct_bal), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    CommitTxn();

    //pOut->status = CBaseTxnErr::SUCCESS;
}
