// DBConnection.cpp
//   2008 Yasufumi Kinoshita

#include "DBConnection.h"

using namespace TPCE;

CDBConnection::CDBConnection(const char *szHost, const char *szDBName,
			     const char *szDBPort, const char *szDBPwd ,const char *szDBUser)
{
    SQLRETURN rc;

#ifdef DEBUG
    cout<<"CDBConnection::CDBConnection"<<endl;
#endif

    m_Env = NULL;
    m_Conn = NULL;
    m_Stmt = m_Stmt2 = NULL;

    //m_PrepareType = szPrepareType;
    m_pPrepared = NULL;

    if ( SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_Env) != SQL_SUCCESS )
	ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_ENV, m_Env);

    if ( SQLSetEnvAttr(m_Env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0 ) != SQL_SUCCESS )
	ThrowError(CODBCERR::eSetEnvAttr);

    if ( SQLAllocHandle(SQL_HANDLE_DBC, m_Env, &m_Conn) != SQL_SUCCESS )
	ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_ENV, m_Env);

    if ( SQLSetConnectAttr(m_Conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0) != SQL_SUCCESS )
	ThrowError(CODBCERR::eSetConnectAttr);

    rc = SQLConnect(m_Conn, (SQLCHAR *)szDBName, SQL_NTS,
		    (SQLCHAR *)szDBUser, SQL_NTS, (SQLCHAR *)szDBPwd, SQL_NTS);

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eConnect);

    if ( SQLAllocHandle(SQL_HANDLE_STMT, m_Conn, &m_Stmt) != SQL_SUCCESS)
	ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_DBC, m_Conn);

    if ( SQLAllocHandle(SQL_HANDLE_STMT, m_Conn, &m_Stmt2) != SQL_SUCCESS)
	ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_DBC, m_Conn);
}

CDBConnection::~CDBConnection()
{
#ifdef DEBUG
    cout<<"CDBConnection::~CDBConnection"<<endl;
#endif

    SQLFreeHandle(SQL_HANDLE_STMT, m_Stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, m_Stmt2);

    SQLEndTran(SQL_HANDLE_DBC, m_Conn, SQL_ROLLBACK);
    SQLDisconnect(m_Conn);
    SQLFreeHandle(SQL_HANDLE_DBC, m_Conn);
}

void CDBConnection::setBrokerageHouse(CBrokerageHouse *bh)
{
	this->bh = bh;
}

void CDBConnection::BeginTxn()
{
}

void CDBConnection::CommitTxn()
{
    SQLRETURN rc;

    rc = SQLEndTran(SQL_HANDLE_DBC, m_Conn, SQL_COMMIT);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eEndTran);
}

void CDBConnection::RollbackTxn()
{
    SQLEndTran(SQL_HANDLE_DBC, m_Conn, SQL_ROLLBACK);
}

void CDBConnection::ThrowError( CODBCERR::ACTION eAction, SQLSMALLINT HandleType, SQLHANDLE Handle,
				const char* FileName, unsigned int Line)
{
    RETCODE     rc;
    SQLINTEGER  lNativeError = 0;
    char        szState[6];
    char        szMsg[SQL_MAX_MESSAGE_LENGTH];
    char        szTmp[6*SQL_MAX_MESSAGE_LENGTH];
    char        szBuff[SQL_MAX_MESSAGE_LENGTH];
    CODBCERR   *pODBCErr;           // not allocated until needed (maybe never)

    pODBCErr = new CODBCERR();

    pODBCErr->m_NativeError = 0;
    pODBCErr->m_eAction = eAction;
    pODBCErr->m_bDeadLock = false;

    if (Handle == SQL_NULL_HANDLE)
    {
        switch (eAction)
        {
	    case CODBCERR::eSetEnvAttr:
		HandleType = SQL_HANDLE_ENV;
		Handle = m_Env;
		break;

	    case CODBCERR::eBcpBind:
	    case CODBCERR::eBcpControl:
	    case CODBCERR::eBcpBatch:
	    case CODBCERR::eBcpDone:
	    case CODBCERR::eBcpInit:
	    case CODBCERR::eBcpSendrow:
	    case CODBCERR::eConnect:
	    case CODBCERR::eSetConnectAttr:
	    case CODBCERR::eEndTran:
		HandleType = SQL_HANDLE_DBC;
		Handle = m_Conn;
		break;

	    case CODBCERR::eBindCol:
	    case CODBCERR::eCloseCursor:
	    case CODBCERR::ePrepare:
	    case CODBCERR::eExecute:
	    case CODBCERR::eExecDirect:
	    case CODBCERR::eFetch:
		HandleType = SQL_HANDLE_STMT;
		Handle = m_Stmt;
		break;
	    default:
		HandleType = SQL_HANDLE_DBC;
		Handle = m_Conn;
        }
    }
    szTmp[0] = 0;

    if(FileName)
    {
	sprintf(szTmp, "[%s:%d] ", FileName, Line);
    }

    int i = 0;
    while (1 && !(pODBCErr->m_bDeadLock))
    {

        rc = SQLGetDiagRec( HandleType, Handle, ++i, (BYTE *)&szState, &lNativeError,
			    (BYTE *)&szMsg, sizeof(szMsg), NULL);
        if (rc == SQL_NO_DATA)
            break;

      
        if (

        lNativeError == 1213 ||

	    strcmp(szState, "40001") == 0)
            pODBCErr->m_bDeadLock = true;

        // capture the (first) database error
        if (pODBCErr->m_NativeError == 0 && lNativeError != 0)
            pODBCErr->m_NativeError = lNativeError;

        // quit if there isn't enough room to concatenate error text
        if ( (strlen(szMsg) + 2) > (sizeof(szTmp) - strlen(szTmp)) )
            break;

        // include line break after first error msg
        if (i != 1)
            strcat( szTmp, "\n");

        sprintf(szBuff, "Native=%d, SQLState=%s : ", (int)lNativeError, szState);
	strcat(szTmp, szBuff);
	if(pODBCErr->m_bDeadLock)
	    strcat(szTmp, "[DEADLOCK]");
	else
	    strcat(szTmp, szMsg);
    }

    if (pODBCErr->m_odbcerrstr != NULL)
    {
        delete [] pODBCErr->m_odbcerrstr;
        pODBCErr->m_odbcerrstr = NULL;
    }

    if (szTmp[0] != 0)
    {
        pODBCErr->m_odbcerrstr = new char[ strlen(szTmp)+1 ];
        strcpy( pODBCErr->m_odbcerrstr, szTmp );
    }

    if(HandleType == SQL_HANDLE_STMT)
	SQLCloseCursor(Handle);

    throw pODBCErr;
}
