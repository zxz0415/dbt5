#include "TradeOrderDB.h"

using namespace TPCE;

CTradeOrderDB::CTradeOrderDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CTradeOrderDB::~CTradeOrderDB()
{
}

void CTradeOrderDB::DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn,
				       TTradeOrderFrame1Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;


    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();

    /* SELECT ca_name, ca_b_id, ca_c_id, ca_tax_st
       FROM customer_account
       WHERE ca_id = %d */

    stmt = m_Stmt;
    ostringstream osTOF1_1;

    osTOF1_1 << "SELECT ca_name, ca_b_id, ca_c_id, ca_tax_st FROM customer_account WHERE ca_id = " <<
	pIn->acct_id;

    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF1_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->acct_name, cCA_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 2, SQL_C_SBIGINT, &(pOut->broker_id), 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 3, SQL_C_SBIGINT, &(pOut->cust_id), 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_LONG, &(pOut->tax_status), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    /* SELECT c_f_name, c_l_name, c_tier, c_tax_id
       FROM customer
       WHERE c_id = %s */


    stmt = m_Stmt;
    ostringstream osTOF1_2;
    osTOF1_2 << "SELECT c_f_name, c_l_name, c_tier, c_tax_id FROM customer WHERE c_id = " <<
	pOut->cust_id;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF1_2.str().c_str()), SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->cust_f_name, cF_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, pOut->cust_l_name, cL_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_LONG, &(pOut->cust_tier), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_CHAR, pOut->tax_id, cTAX_ID_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    if (rc == SQL_NO_DATA_FOUND) {
	//pOut->status = -711;
	return;
    }

    /* SELECT b_name
       FROM broker
       WHERE b_id = %s */


    stmt = m_Stmt;
    ostringstream osTOF1_3;
    osTOF1_3 << "SELECT b_name FROM broker WHERE b_id = " <<
	pOut->broker_id;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF1_3.str().c_str()), SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->broker_name, cB_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    //pOut->status = CBaseTxnErr::SUCCESS;
    pOut->num_found = 1;
}

void CTradeOrderDB::DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn,
				       TTradeOrderFrame2Output *pOut)
{
    SQLHSTMT stmt;
    SQLRETURN rc;

    //pOut->status = CBaseTxnErr::SUCCESS;

    /* SELECT ap_acl
       FROM account_permission
       WHERE ap_ca_id = %d
         AND ap_f_name = '%s'
         AND ap_l_name = '%s'
         AND ap_tax_id = '%s' */

    stmt = m_Stmt;
    ostringstream osTOF2_1;
    osTOF2_1 << "SELECT ap_acl FROM account_permission WHERE ap_ca_id = " <<
	pIn->acct_id << " AND ap_f_name = '" <<
	pIn->exec_f_name << "' AND ap_l_name = '" <<
	pIn->exec_l_name << "' AND ap_tax_id = '" <<
	pIn->exec_tax_id << "'";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF2_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->ap_acl, cACL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    if (rc == SQL_NO_DATA_FOUND) //=== SQL_NO_DATA_FOUND is allowed.
    {
	//This may not be needed...? (Ref: inc/TxnHarnessTradeOrder.h)
	RollbackTxn();
	//pOut->status = -721;
    }
}

void CTradeOrderDB::DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn,
				       TTradeOrderFrame3Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    TIdent co_id;
    char exch_id[cEX_ID_len+1];
    double hold_price;
    INT32 hold_qty;
    INT32 needed_qty;
    INT32 hs_qty;

    if (pIn->symbol[0] == 0) //""
    {
	strncpy(pOut->co_name, pIn->co_name, cCO_NAME_len+1);

	/* SELECT co_id
	   FROM company
	   WHERE co_name = '%s' */

	char co_name[cCO_NAME_len+1];
	expand_quote(co_name, pIn->co_name, cCO_NAME_len+1);

	stmt = m_Stmt;
	ostringstream osTOF3_1a;

	osTOF3_1a << "SELECT co_id FROM company WHERE co_name = '" <<
	    co_name << "'";

	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_1a.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &co_id, 0, NULL);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	/* SELECT s_ex_id, s_name, s_symb
	   FROM security
	   WHERE s_co_id = %s
	     AND s_issue = '%s' */

	stmt = m_Stmt;
	ostringstream osTOF3_2a;
	osTOF3_2a << "SELECT s_ex_id, s_name, s_symb FROM security WHERE s_co_id = " <<
	    co_id << " AND s_issue = '" <<
	    pIn->issue << "'";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_2a.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_CHAR, exch_id, cEX_ID_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 2, SQL_C_CHAR, pOut->s_name, cS_NAME_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->symbol, cSYMBOL_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);
    }
    else
    {
	strncpy(pOut->symbol, pIn->symbol, cSYMBOL_len+1);

	/* SELECT s_co_id, s_ex_id, s_name
	   FROM security
	   WHERE s_symb = '%s' */
	stmt = m_Stmt;
	ostringstream osTOF3_1b;

	osTOF3_1b << "SELECT s_co_id, s_ex_id, s_name FROM security WHERE s_symb = '" <<
	    pIn->symbol << "'";

	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_1b.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &co_id, 0, NULL);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 2, SQL_C_CHAR, exch_id, cEX_ID_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->s_name, cS_NAME_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	/* SELECT co_name
	   FROM company
	   WHERE co_id = %s */

	stmt = m_Stmt;
	ostringstream osTOF3_2b;
	osTOF3_2b << "SELECT co_name FROM company WHERE co_id = " <<
	    co_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_2b.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->co_name, cCO_NAME_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);
    }


    /* SELECT lt_price
       FROM last_trade
       WHERE lt_s_symb = '%s' */

    stmt = m_Stmt;
    ostringstream osTOF3_3;
    osTOF3_3 << "SELECT lt_price FROM last_trade WHERE lt_s_symb = '" <<
	pOut->symbol << "'";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_3.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->market_price), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /* SELECT tt_is_mrkt, tt_is_sell
       FROM trade_type
       WHERE tt_id = '%s' */

    stmt = m_Stmt;
    ostringstream osTOF3_4;
    osTOF3_4 << "SELECT tt_is_mrkt, tt_is_sell FROM trade_type WHERE tt_id = '" <<
	pIn->trade_type_id << "'";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_4.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_LONG, &(pOut->type_is_market), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_LONG, &(pOut->type_is_sell), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    if ( pOut->type_is_market )
    {
	pOut->requested_price = pOut->market_price;
    }
    else
    {
	pOut->requested_price = pIn->requested_price;
    }

    pOut->buy_value = 0.0;
    pOut->sell_value = 0.0;
    needed_qty = pIn->trade_qty;

    /* SELECT hs_qty
       FROM holding_summary
       WHERE hs_ca_id = %d
         AND hs_s_symb = '%s' */

    stmt = m_Stmt;
    ostringstream osTOF3_5;
    osTOF3_5 << "SELECT hs_qty FROM holding_summary WHERE hs_ca_id = " <<
	pIn->acct_id << " AND hs_s_symb = '" <<
	pOut->symbol << "'";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_5.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_LONG, &hs_qty, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    if (rc == SQL_NO_DATA_FOUND) //=== SQL_NO_DATA_FOUND is allowed.
	hs_qty = 0;

    if ( pOut->type_is_sell )
    {
	if ( hs_qty > 0 )
	{
	    /* SELECT h_qty, h_price
	       FROM holding
	       WHERE h_ca_id = %d
	         AND h_s_symb = '%s'
	       ORDER BY h_dts [ DESC | ASC ] */

	    stmt = m_Stmt;
	    ostringstream osTOF3_6;
	    osTOF3_6 << "SELECT h_qty, h_price FROM holding WHERE h_ca_id = " <<
		pIn->acct_id << " AND h_s_symb = '" <<
		pOut->symbol << "' ORDER BY h_dts ";

	    if ( pIn->is_lifo )
		osTOF3_6 << "DESC";
	    else
		osTOF3_6 << "ASC";

	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_6.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt, 1, SQL_C_LONG, &hold_qty, 0, NULL);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &hold_price, 0, NULL);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLFetch(stmt);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	    while( needed_qty != 0 && rc != SQL_NO_DATA_FOUND )
	    {
		if ( hold_qty > needed_qty )
		{
		    pOut->buy_value  += needed_qty * hold_price;
		    pOut->sell_value += needed_qty * (pOut->requested_price);
		    needed_qty = 0;
		}
		else
		{
		    pOut->buy_value  += hold_qty * hold_price;
		    pOut->sell_value += hold_qty * (pOut->requested_price);
		    needed_qty = needed_qty - hold_qty;
		}

		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    }
	    SQLCloseCursor(stmt);

	}
    }
    else
    {
	if (hs_qty < 0)
	{
	    /* SELECT h_qty, h_price
	       FROM holding
	       WHERE h_ca_id = %d
	         AND h_s_symb = '%s'
	       ORDER BY h_dts [ DESC | ASC ] */
	    stmt = m_Stmt;
	    ostringstream osTOF3_6;
	    osTOF3_6 << "SELECT h_qty, h_price FROM holding WHERE h_ca_id = " <<
		pIn->acct_id << " AND h_s_symb = '" <<
		pOut->symbol << "' ORDER BY h_dts ";

	    if ( pIn->is_lifo )
		osTOF3_6 << "DESC";
	    else
		osTOF3_6 << "ASC";

	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_6.str().c_str()), SQL_NTS);


	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt, 1, SQL_C_LONG, &hold_qty, 0, NULL);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &hold_price, 0, NULL);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLFetch(stmt);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	    while( needed_qty != 0 && rc != SQL_NO_DATA_FOUND )
	    {
		if ( hold_qty + needed_qty > 0 )
		{
		    pOut->sell_value  += needed_qty * hold_price;
		    pOut->buy_value += needed_qty * (pOut->requested_price);
		    needed_qty = 0;
		}
		else
		{
		    hold_qty = -hold_qty;
		    pOut->sell_value  += hold_qty * hold_price;
		    pOut->buy_value += hold_qty * (pOut->requested_price);
		    needed_qty = needed_qty - hold_qty;
		}

		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    }
	    SQLCloseCursor(stmt);

	}
    }

    pOut->tax_amount = 0.0;
    if ((pOut->sell_value > pOut->buy_value)
	&& ((pIn->tax_status == 1) || (pIn->tax_status == 2)))
    {
	double tax_rates;

	/* SELECT sum(tx_rate)
	   FROM taxrate
	   WHERE tx_id in (
	                   SELECT cx_tx_id
	                   FROM customer_taxrate
	                   WHERE cx_c_id = %d) */
	//OR
	/* SELECT sum(tx_rate)
	   FROM taxrate, customer_taxrate
	   WHERE tx_id = cx_tx_id
	     AND cx_c_id = %d */
	stmt = m_Stmt;
	ostringstream osTOF3_7;
	osTOF3_7 << "SELECT sum(tx_rate) FROM taxrate, customer_taxrate WHERE tx_id = cx_tx_id AND cx_c_id = " <<
	    pIn->cust_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_7.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &tax_rates, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	pOut->tax_amount = (pOut->sell_value - pOut->buy_value) * tax_rates;
    }

    /* SELECT cr_rate
       FROM commission_rate
       WHERE cr_c_tier = %d
         AND cr_tt_id = '%s'
         AND cr_ex_id = '%s' 
         AND cr_from_qty <= %d
         AND cr_to_qty >= %d */

    stmt = m_Stmt;
    ostringstream osTOF3_8;
    osTOF3_8 << "SELECT cr_rate FROM commission_rate WHERE cr_c_tier = " <<
	pIn->cust_tier << " AND cr_tt_id = '" <<
	pIn->trade_type_id << "' AND cr_ex_id = '" <<
	exch_id << "' AND cr_from_qty <= " <<
	pIn->trade_qty << " AND cr_to_qty >= " <<
	pIn->trade_qty;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_8.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->comm_rate), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /*  SELECT ch_chrg
	FROM charge
	WHERE ch_c_tier = %d
	  AND ch_tt_id = '%s' */

    stmt = m_Stmt;
    ostringstream osTOF3_9;
    osTOF3_9 << "SELECT ch_chrg FROM charge WHERE ch_c_tier = " <<
	pIn->cust_tier << " AND ch_tt_id = '" <<
	pIn->trade_type_id << "'";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_9.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->charge_amount), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    double acct_bal;
    double hold_assets;

    //pOut->cust_assets = 0.0;
    if ( pIn->type_is_margin )
    {
	/* SELECT ca_bal
	   FROM customer_account
	   WHERE ca_id = %d */

	stmt = m_Stmt;
	ostringstream osTOF3_10;
	osTOF3_10 << "SELECT ca_bal FROM customer_account WHERE ca_id = " <<
	    pIn->acct_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_10.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &acct_bal, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);


	/* SELECT sum(hs_qty * lt_price)
	   FROM holding_summary, last_trade
	   WHERE hs_ca_id = %d
	     AND lt_s_symb = hs_s_symb */

	stmt = m_Stmt;
	ostringstream osTOF3_11;
	osTOF3_11 << "SELECT sum(hs_qty * lt_price) FROM holding_summary, last_trade WHERE hs_ca_id = " <<
	    pIn->acct_id << " AND lt_s_symb = hs_s_symb";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF3_11.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &hold_assets, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	//if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /*&& rc != SQL_NO_DATA_FOUND*/)
	//    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	/*if (rc == SQL_NO_DATA_FOUND) //=== SQL_NO_DATA_FOUND is allowed.
	    pOut->cust_assets = acct_bal;
	else
	    pOut->cust_assets = hold_assets + acct_bal;*/
    }

    if ( pOut->type_is_market )
	strncpy( pOut->status_id, pIn->st_submitted_id, cST_ID_len+1);
    else
	strncpy( pOut->status_id, pIn->st_pending_id, cST_ID_len+1);

    //pOut->status = CBaseTxnErr::SUCCESS;
}

void CTradeOrderDB::DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn,
				       TTradeOrderFrame4Output *pOut)
{

    SQLHSTMT stmt;

    SQLRETURN rc;

    char  now_dts[TIMESTAMP_LEN+1];

    gettimestamp(now_dts, STRFTIME_FORMAT, TIMESTAMP_LEN);

    //***************************************
    //* get_new_trade_id ( pOut->trade_id ) *
    //***************************************


    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"UPDATE seq_trade_id SET id=LAST_INSERT_ID(id+1)", SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);
    stmt = m_Stmt;

    rc = SQLExecDirect(stmt, (SQLCHAR*)"SELECT LAST_INSERT_ID()", SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &(pOut->trade_id), 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /* INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash,
                         t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name,
                         t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
       VALUES (%ld, '%s', '%s', '%s', %d, '%s',
               %d, %8.2f, %d, '%s', NULL, %10.2f, %10.2f, 0, %d) */

    char requested_price[8+2+3];
    char charge_amount[10+2+3];
    char comm_amount[10+2+3];
    snprintf(requested_price, 8+2+3, "%8.2f", pIn->requested_price);
    snprintf(charge_amount, 10+2+3, "%10.2f", pIn->charge_amount);
    snprintf(comm_amount, 10+2+3, "%10.2f", pIn->comm_amount);

    stmt = m_Stmt;
    ostringstream osTOF4_1;
    osTOF4_1 << "INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_trade_price, t_chrg, t_comm, t_tax, t_lifo) VALUES (" <<
	pOut->trade_id << ", '" <<
	now_dts << "', '" <<
	pIn->status_id << "', '" <<
	pIn->trade_type_id << "', " <<

	pIn->is_cash << ", '" <<

	pIn->symbol << "', " <<
	pIn->trade_qty << ", " <<
	requested_price << ", " <<
	pIn->acct_id << ", '" <<
	pIn->exec_name << "', NULL, " <<
	charge_amount << ", " <<
	comm_amount << ", 0, " <<
	pIn->is_lifo << ")";

    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF4_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    if ( !(pIn->type_is_market) )
    {
	/* INSERT INTO trade_request(tr_t_id, tr_tt_id, tr_s_symb, tr_qty,
	                             tr_bid_price, tr_b_id)
	   VALUES (%s, '%s', '%s', %d, %8.2f, %d) */

	stmt = m_Stmt;
	ostringstream osTOF4_2;
	osTOF4_2 << "INSERT INTO trade_request(tr_t_id, tr_tt_id, tr_s_symb, tr_qty, tr_bid_price, tr_b_id) VALUES (" <<
	    pOut->trade_id << ", '" <<
	    pIn->trade_type_id << "', '" <<
	    pIn->symbol << "', " <<
	    pIn->trade_qty << ", " <<
	    requested_price << ", " <<
	    pIn->broker_id << ")";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF4_2.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);
    }

    /* INSERT INTO trade_history(th_t_id, th_dts, th_st_id)
       VALUES (%s, '%s', '%s') */

    stmt = m_Stmt;
    ostringstream osTOF4_3;
    osTOF4_3 << "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) VALUES (" <<
	pOut->trade_id << ", '" <<
	now_dts << "', '" <<
	pIn->status_id << "')";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTOF4_3.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    //pOut->status = CBaseTxnErr::SUCCESS;
}

void CTradeOrderDB::DoTradeOrderFrame5(/*TTradeOrderFrame5Output *pOut*/)
{
    RollbackTxn();

    //pOut->status = CBaseTxnErr::SUCCESS;
}

void CTradeOrderDB::DoTradeOrderFrame6(/*TTradeOrderFrame6Output *pOut*/)
{
    CommitTxn();

    //pOut->status = CBaseTxnErr::SUCCESS;
}
