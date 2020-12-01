#include "TradeStatusDB.h"

using namespace TPCE;

CTradeStatusDB::CTradeStatusDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CTradeStatusDB::~CTradeStatusDB()
{
}

void CTradeStatusDB::DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn,
					 TTradeStatusFrame1Output *pOut)
{
#ifdef DEBUG
    cout<<"CTradeStatusDB::DoTradeStatusFrame1"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    TTrade  trade_id;
    char    datetime_buf[30]; //pOut->trade_dts
    char    status_name[cST_NAME_len+1];
    char    type_name[cTT_NAME_len+1];
    char    symbol[cSYMBOL_len+1];
    INT32   trade_qty;
    char    exec_name[cEXEC_NAME_len+1];
    double  charge;
    char    s_name[cS_NAME_len+1];
    char    ex_name[cEX_NAME_len+1];

    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();


    /* SELECT t_id, DATE_FORMAT(t_dts,'%Y-%m-%d %H:%i:%s.%f'), st_name, tt_name, t_s_symb, t_qty, 
              t_exec_name, t_chrg, s_name, ex_name
       FROM trade, status_type, trade_type, security, exchange
       WHERE t_ca_id = %d
         AND st_id = t_st_id
         AND tt_id = t_tt_id
         AND s_symb = t_s_symb
         AND ex_id = s_ex_id
       ORDER BY t_dts DESC
       LIMIT max_trade_status_len */

    stmt = m_Stmt;
    ostringstream osTSF1_1;

    osTSF1_1 << "SELECT t_id, DATE_FORMAT(t_dts,'%Y-%m-%d %H:%i:%s.%f'), st_name, tt_name, t_s_symb, t_qty, t_exec_name, t_chrg, s_name, ex_name FROM trade, status_type FORCE INDEX(PRIMARY), trade_type FORCE INDEX(PRIMARY), security, exchange WHERE t_ca_id = " <<
	pIn->acct_id << " AND st_id = t_st_id AND tt_id = t_tt_id AND s_symb = t_s_symb AND ex_id = s_ex_id ORDER BY t_dts DESC LIMIT " <<
	max_trade_status_len;

    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTSF1_1.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, __FILE__, __LINE__);

    rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &trade_id, 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, datetime_buf, 30, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_CHAR, status_name, cST_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_CHAR, type_name, cTT_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_CHAR, symbol, cSYMBOL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 6, SQL_C_LONG, &trade_qty, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 7, SQL_C_CHAR, exec_name, cEXEC_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 8, SQL_C_DOUBLE, &charge, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 9, SQL_C_CHAR, s_name, cS_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 10, SQL_C_CHAR, ex_name, cEX_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
        ThrowError(CODBCERR::eFetch, __FILE__, __LINE__);

    int i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < max_trade_status_len)
    {
	pOut->trade_id[i] = trade_id;
	sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu;%2hu.%u",
	       &(pOut->trade_dts[i].year),
	       &(pOut->trade_dts[i].month),
	       &(pOut->trade_dts[i].day),
	       &(pOut->trade_dts[i].hour),
	       &(pOut->trade_dts[i].minute),
	       &(pOut->trade_dts[i].second),
	       &(pOut->trade_dts[i].fraction));
	pOut->trade_dts[i].fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
	strncpy(pOut->status_name[i], status_name, cST_NAME_len+1);
	strncpy(pOut->type_name[i], type_name, cTT_NAME_len+1);
	strncpy(pOut->symbol[i], symbol, cSYMBOL_len+1);
	pOut->trade_qty[i] = trade_qty;
	strncpy(pOut->exec_name[i], exec_name, cEXEC_NAME_len+1);
	pOut->charge[i] = charge;
	strncpy(pOut->s_name[i], s_name, cS_NAME_len+1);
	strncpy(pOut->ex_name[i], ex_name, cEX_NAME_len+1);
	i++;

	rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

   pOut->num_found = 50;
    /*if (i == 50) {
	pOut->status = CBaseTxnErr::SUCCESS;
    } else {
	pOut->status = -911;
    }*/

    /* SELECT c_l_name, c_f_name, b_name
       FROM customer_account, customer, broker
       WHERE ca_id = %d
         AND c_id = ca_c_id
         AND b_id = ca_b_id */

    stmt = m_Stmt;
    ostringstream osTSF1_2;
    osTSF1_2 << "SELECT c_l_name, c_f_name, b_name FROM customer_account, customer, broker WHERE ca_id = " <<
	pIn->acct_id << " AND c_id = ca_c_id AND b_id = ca_b_id";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTSF1_2.str().c_str()), SQL_NTS);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->cust_l_name, cL_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, pOut->cust_f_name, cF_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->broker_name, cB_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
        ThrowError(CODBCERR::eFetch, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    CommitTxn();

}
