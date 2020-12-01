#include "TradeCleanupDB.h"

using namespace TPCE;

CTradeCleanupDB::CTradeCleanupDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CTradeCleanupDB::~CTradeCleanupDB()
{
}

void CTradeCleanupDB::DoTradeCleanupFrame1(
    const TTradeCleanupFrame1Input *pIn/*,
    TTradeCleanupFrame1Output *pOut*/)
{

    SQLHSTMT stmt;
    SQLHSTMT stmt2;

    SQLRETURN rc;

    TTrade t_id; /*INT64*/
    TTrade tr_t_id; /*INT64*/
    char   now_dts[TIMESTAMP_LEN+1];

    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();


    /*	OPEN pending_list FOR
	SELECT  TR_T_ID
	FROM    TRADE_REQUEST
	ORDER BY TR_T_ID; */

    stmt2 = m_Stmt2;
    ostringstream osTCF1_1;

    osTCF1_1 << "SELECT tr_t_id FROM trade_request ORDER BY tr_t_id";
    rc = SQLExecDirect(stmt2, (SQLCHAR*)(osTCF1_1.str().c_str()), SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

    rc = SQLBindCol(stmt2, 1, SQL_C_SBIGINT, &tr_t_id, 0, NULL);


    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

    rc = SQLFetch(stmt2);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


    while (rc != SQL_NO_DATA_FOUND)
    {
	gettimestamp(now_dts, STRFTIME_FORMAT, TIMESTAMP_LEN);

	try
	{

	/*	INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
		VALUES (tr_t_id, now_dts, st_submitted_id); */

	stmt = m_Stmt;
	ostringstream osTCF1_2;
	osTCF1_2 << "INSERT INTO trade_history (th_t_id, th_dts, th_st_id) VALUES (" << tr_t_id <<
	    ", '" << now_dts << "', '" << pIn->st_submitted_id <<
	    "')";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_2.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);


	/*	UPDATE  TRADE
		SET     T_ST_ID = st_canceled_id,
		T_DTS = now_dts
		WHERE   T_ID = tr_t_id; */

	stmt = m_Stmt;
	ostringstream osTCF1_5;
	osTCF1_5 << "UPDATE trade SET t_st_id = '" << pIn->st_canceled_id <<
	    "', t_dts = '" << now_dts <<
	    "' WHERE t_id = " << tr_t_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_5.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);


	/*	INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
		VALUES (tr_t_id, now_dts, st_canceled_id); */
	stmt = m_Stmt;
	ostringstream osTCF1_6;
	osTCF1_6 << "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) VALUES (" << tr_t_id <<
	    ", '" << now_dts << "', '" << pIn->st_canceled_id <<
	    "')";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_6.str().c_str()), SQL_NTS);


	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	}
	catch (const CODBCERR* e)
	{
	    SQLCloseCursor(stmt2);
	    throw;
	}

	/*	FETCH   pending_list
		INTO    tr_trade_id; */

	rc = SQLFetch(stmt2);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt2);


    /*	DELETE FROM TRADE_REQUEST; */

    stmt = m_Stmt;
    ostringstream osTCF1_3;
    osTCF1_3 << "DELETE FROM trade_request";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_3.str().c_str()), SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /*	OPEN submit_list FOR
	SELECT  T_ID
	FROM    TRADE
	WHERE   T_ID >= start_trade_id AND
	        T_ST_ID = st_submitted_id; */

    stmt2 = m_Stmt2;
    ostringstream osTCF1_4;

    osTCF1_4 << "SELECT t_id FROM trade WHERE t_id >= " << pIn->start_trade_id <<

	" AND t_st_id = '" << pIn->st_submitted_id <<
	"'";
    rc = SQLExecDirect(stmt2, (SQLCHAR*)(osTCF1_4.str().c_str()), SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


    /*	FETCH   submit_list
	INTO    t_id; */

    rc = SQLBindCol(stmt2, 1, SQL_C_SBIGINT, &t_id, 0, NULL);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

    rc = SQLFetch(stmt2);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


    while (rc != SQL_NO_DATA_FOUND)
    {
	gettimestamp(now_dts, STRFTIME_FORMAT, TIMESTAMP_LEN);

	try
	{

	/*	UPDATE  TRADE
		SET     T_ST_ID = st_canceled_id,
		        T_DTS = now_dts
		WHERE   T_ID = t_id; */


	stmt = m_Stmt;
	ostringstream osTCF1_5;
	osTCF1_5 << "UPDATE trade SET t_st_id = '" << pIn->st_canceled_id <<
	    "', t_dts = '" << now_dts <<
	    "' WHERE t_id = " << t_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_5.str().c_str()), SQL_NTS);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);


	/*	INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
		VALUES (t_id, now_dts, st_canceled_id); */

	stmt = m_Stmt;
	ostringstream osTCF1_6;
	osTCF1_6 << "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) VALUES (" << t_id <<
	    ", '" << now_dts << "', '" << pIn->st_canceled_id <<
	    "')";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_6.str().c_str()), SQL_NTS);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	}
	catch (const CODBCERR* e)
	{
	    SQLCloseCursor(stmt2);
	    throw;
	}

	/*	FETCH   submit_list
		INTO    t_id; */

	rc = SQLFetch(stmt2);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt2);

    CommitTxn();

    //pOut->status = CBaseTxnErr::SUCCESS;
}
