// CustomerPositionDB.cpp
//   2008 Yasufumi Kinoshita

#include "CustomerPositionDB.h"

using namespace TPCE;

CCustomerPositionDB::CCustomerPositionDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CCustomerPositionDB::~CCustomerPositionDB()
{
}

void CCustomerPositionDB::DoCustomerPositionFrame1(
    const TCustomerPositionFrame1Input *pIn,
    TCustomerPositionFrame1Output *pOut)
{
    SQLHSTMT stmt;
    SQLRETURN rc;
    TIdent cust_id;
    INT32  c_tier;
    char   date_buf[15];
    TIdent acct_id;
    double cash_bal;
    double asset_total;

    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();

    if(pIn->cust_id == 0)
    {
	/* SELECT c_id
	   FROM   customer
	   WHERE  c_tax_id = '%s' */

	stmt = m_Stmt;
	ostringstream osCPF1_1;

	osCPF1_1 << "SELECT c_id FROM customer WHERE  c_tax_id = '" << pIn->tax_id <<
	    "'";

	rc = SQLExecDirect(stmt, (SQLCHAR*)(osCPF1_1.str().c_str()), SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &cust_id, 0, NULL);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);
    }
    else
    {
	cust_id = pIn->cust_id;
    }

    /* SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr,
              c_tier, DATE_FORMAT(c_dob,'%Y-%m-%d'), c_ad_id, c_ctry_1, c_area_1,
              c_local_1, c_ext_1, c_ctry_2, c_area_2, c_local_2,
              c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3,
              c_email_1, c_email_2
       FROM   customer
       WHERE  c_id = %ld */


    stmt = m_Stmt;
    ostringstream osCPF1_2;

    osCPF1_2 << "SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, DATE_FORMAT(c_dob,'%Y-%m-%d'), c_ad_id, c_ctry_1, c_area_1, c_local_1, c_ext_1, c_ctry_2, c_area_2, c_local_2, c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3, c_email_1, c_email_2 FROM customer WHERE c_id = " << cust_id;

    rc = SQLExecDirect(stmt, (SQLCHAR*)(osCPF1_2.str().c_str()), SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->c_st_id, cST_ID_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, pOut->c_l_name, cL_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->c_f_name, cF_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_CHAR, pOut->c_m_name, cM_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_CHAR, pOut->c_gndr, cGNDR_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 6, SQL_C_LONG, &c_tier, 0, NULL); //pOut->c_tier
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 7, SQL_C_CHAR, date_buf, 15, NULL); //pOut->c_dob
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 8, SQL_C_SBIGINT, &(pOut->c_ad_id), 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 9, SQL_C_CHAR, pOut->c_ctry_1, cCTRY_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 10, SQL_C_CHAR, pOut->c_area_1, cAREA_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 11, SQL_C_CHAR, pOut->c_local_1, cLOCAL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 12, SQL_C_CHAR, pOut->c_ext_1, cEXT_len+1, &m_DummyInd);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 13, SQL_C_CHAR, pOut->c_ctry_2, cCTRY_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 14, SQL_C_CHAR, pOut->c_area_2, cAREA_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 15, SQL_C_CHAR, pOut->c_local_2, cLOCAL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 16, SQL_C_CHAR, pOut->c_ext_2, cEXT_len+1, &m_DummyInd);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 17, SQL_C_CHAR, pOut->c_ctry_3, cCTRY_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 18, SQL_C_CHAR, pOut->c_area_3, cAREA_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 19, SQL_C_CHAR, pOut->c_local_3, cLOCAL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 20, SQL_C_CHAR, pOut->c_ext_3, cEXT_len+1, &m_DummyInd);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 21, SQL_C_CHAR, pOut->c_email_1, cEMAIL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 22, SQL_C_CHAR, pOut->c_email_2, cEMAIL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    pOut->c_tier = (char)c_tier;

    //pOut->c_dob = date_buf;
    sscanf(date_buf,"%4hd-%2hu-%2hu",
	   &(pOut->c_dob.year),
	   &(pOut->c_dob.month),
	   &(pOut->c_dob.day));
    pOut->c_dob.hour = pOut->c_dob.minute = pOut->c_dob.second = 0;
    pOut->c_dob.fraction = 0;


    /* SELECT   ca_id,
                ca_bal,
                COALESCE(SUM(hs_qty * lt_price),0)
       FROM     customer_account
                LEFT OUTER JOIN holding_summary
                             ON hs_ca_id = ca_id,
                last_trade
       WHERE    ca_c_id = %ld
                AND lt_s_symb = hs_s_symb
       GROUP BY ca_id,ca_bal
       ORDER BY 3 ASC
       LIMIT max_acct_len */

    stmt = m_Stmt;
    ostringstream osCPF1_3;

    osCPF1_3 << "SELECT ca_id, ca_bal, COALESCE(SUM(hs_qty * lt_price),0) AS price_sum FROM customer_account LEFT OUTER JOIN holding_summary ON hs_ca_id = ca_id, last_trade WHERE ca_c_id = " <<
	cust_id << " AND lt_s_symb = hs_s_symb GROUP BY ca_id,ca_bal ORDER BY price_sum ASC LIMIT " <<
	max_acct_len;

    rc = SQLExecDirect(stmt, (SQLCHAR*)(osCPF1_3.str().c_str()), SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &acct_id, 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &cash_bal, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_DOUBLE, &asset_total, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    int i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < max_acct_len)
    {

	pOut->acct_id[i] = acct_id;
	pOut->cash_bal[i] = cash_bal;
	pOut->asset_total[i] = asset_total;
	i++;

	rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    pOut->acct_len = i;
    //pOut->status = CBaseTxnErr::SUCCESS;

}

void CCustomerPositionDB::DoCustomerPositionFrame2(
    const TCustomerPositionFrame2Input *pIn,
    TCustomerPositionFrame2Output *pOut)
{
    SQLHSTMT stmt;

    SQLRETURN rc;

    TTrade trade_id;
    char   symbol[cSYMBOL_len+1];
    INT32  qty;
    char   trade_status[cST_NAME_len+1];
    char   datetime_buf[30]; //hist_dts

    /* SELECT   t_id,
                t_s_symb,
                t_qty,
                st_name,
                DATE_FORMAT(th_dts,'%Y-%m-%d %H:%i:%s.%f')
       FROM     (SELECT   t_id AS id
                 FROM     trade
                 WHERE    t_ca_id = %ld
                 ORDER BY t_dts DESC
                 LIMIT 10) AS t,
                trade,
                trade_history,
                status_type
       WHERE    t_id = id
                AND th_t_id = t_id
                AND st_id = th_st_id
       ORDER BY th_dts DESC
       LIMIT max_hist_len */




    stmt = m_Stmt;
    ostringstream osCPF2_1;

    osCPF2_1 << "SELECT t_id, t_s_symb, t_qty, st_name, DATE_FORMAT(th_dts,'%Y-%m-%d %H:%i:%s.%f') FROM (SELECT t_id AS id FROM trade WHERE t_ca_id = " <<
	pIn->acct_id << " ORDER BY t_dts DESC LIMIT 10) AS t, trade, trade_history, status_type FORCE INDEX(PRIMARY) WHERE t_id = id AND th_t_id = t_id AND st_id = th_st_id ORDER BY th_dts DESC LIMIT " <<
	max_hist_len;

    rc = SQLExecDirect(stmt, (SQLCHAR*)(osCPF2_1.str().c_str()), SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &trade_id, 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, symbol, cSYMBOL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_LONG, &qty, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_CHAR, trade_status, cST_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_CHAR, datetime_buf, 30, NULL); //hist_dts
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    int i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < max_hist_len)
    {
	pOut->trade_id[i] = trade_id;
	strncpy(pOut->symbol[i], symbol, cSYMBOL_len+1);
	pOut->qty[i] = qty;
	strncpy(pOut->trade_status[i], trade_status, cST_NAME_len+1);
	sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
	       &(pOut->hist_dts[i].year),
	       &(pOut->hist_dts[i].month),
	       &(pOut->hist_dts[i].day),
	       &(pOut->hist_dts[i].hour),
	       &(pOut->hist_dts[i].minute),
	       &(pOut->hist_dts[i].second),
	       &(pOut->hist_dts[i].fraction));
	pOut->hist_dts[i].fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
	i++;

	rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    CommitTxn();

    pOut->hist_len = i;
    //pOut->status = CBaseTxnErr::SUCCESS;
}

void CCustomerPositionDB::DoCustomerPositionFrame3(
    /*TCustomerPositionFrame3Output *pOut*/)
{

    CommitTxn();

    //pOut->status = CBaseTxnErr::SUCCESS;
}
