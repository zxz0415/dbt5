#include "BaseInterface.h"
#include "DBT5Consts.h"

CBaseInterface::CBaseInterface(char *addr, const int iListenPort,
		ofstream *pflog, ofstream *pfmix, CMutex *pLogLock,
		CMutex *pMixLock)
: m_szBHAddress(addr),
  m_iBHlistenPort(iListenPort),
  m_pLogLock(pLogLock),
  m_pMixLock(pMixLock),
  m_pfLog(pflog),
  m_pfMix(pfmix)
{
	sock = new CSocket(m_szBHAddress, m_iBHlistenPort);
	biConnect();
}

// destructor
CBaseInterface::~CBaseInterface()
{
	biDisconnect();
	delete sock;
}

// connect to BrokerageHouse
bool CBaseInterface::biConnect()
{
	try {
		sock->dbt5Connect();
		return true;
	} catch(CSocketErr *pErr) {
		ostringstream osErr;
		osErr << "Error: " << pErr->ErrorText() <<
				" at CBaseInterface::talkToSUT " << endl;
		logErrorMessage(osErr.str());
		return false;
	}
}

// close connection to BrokerageHouse
bool CBaseInterface::biDisconnect()
{
	try {
		sock->dbt5Disconnect();
		return true;
	} catch(CSocketErr *pErr) {
		ostringstream osErr;
		osErr << "Error: " << pErr->ErrorText() <<
				" at CBaseInterface::talkToSUT " << endl;
		logErrorMessage(osErr.str());
		return false;
	}
}

// Connect to BrokerageHouse, send request, receive reply, and calculate RT
bool CBaseInterface::talkToSUT(PMsgDriverBrokerage pRequest)
{
	int length = 0;
	TMsgBrokerageDriver Reply; // reply message from BrokerageHouse
	memset(&Reply, 0, sizeof(Reply));

	CDateTime StartTime, EndTime, TxnTime; // to time the transaction

	// record txn start time -- please, see TPC-E specification clause

	StartTime.SetToCurrent();

	// send and wait for response
	try {
		length = sock->dbt5Send(reinterpret_cast<void *>(pRequest),
				sizeof(*pRequest));
	} catch(CSocketErr *pErr) {
		sock->dbt5Reconnect();
		logResponseTime(-1, 0, -1);

		ostringstream msg;
		msg << time(NULL) << " " << (long long) pthread_self() << " " <<
				szTransactionName[pRequest->TxnType] << ": " << endl <<
				"Error sending " << length << " bytes of data" << endl <<
				pErr->ErrorText() << endl;
		logErrorMessage(msg.str());
		length = -1;
		delete pErr;
	}
	try {
		length = sock->dbt5Receive(reinterpret_cast<void *>(&Reply),
				sizeof(Reply));
	} catch(CSocketErr *pErr) {
		logResponseTime(-1, 0, -2);

		ostringstream msg;
		msg << time(NULL) << " " << (long long) pthread_self() << " " <<
				szTransactionName[pRequest->TxnType] << ": " << endl <<
				"Error receiving " << length << " bytes of data" << endl <<
				pErr->ErrorText() << endl;
		logErrorMessage(msg.str());
		length = -1;
		if (pErr->getAction() == CSocketErr::ERR_SOCKET_CLOSED)
			sock->dbt5Reconnect();
		delete pErr;
	}

	// record txn end time
	EndTime.SetToCurrent();

	// calculate txn response time
	TxnTime.Set(0); // clear time
	TxnTime.Add(0, (int) ((EndTime - StartTime) * MsPerSecond)); // add ms

	//log response time
	//logResponseTime(Reply.iStatus, pRequest->TxnType, TxnTime.MSec() / 1000.0);

	if (Reply.iStatus == CBaseTxnErr::SUCCESS){
		logResponseTime(Reply.iStatus, pRequest->TxnType, TxnTime.MSec() / 1000.0);
		return true;
		}
	return false;
}

// Log Transaction Response Times
void CBaseInterface::logResponseTime(int iStatus, int iTxnType, double dRT)
{
	m_pMixLock->lock();
	//*(m_pfMix) << (long long) time(NULL) << "," << iTxnType << "," <<
	//		iStatus << "," << dRT << "," << (long long) pthread_self() << endl;
	  *(m_pfMix) << (long long) time(NULL)-1550000000 << " " << dRT << " " <<iTxnType << endl;
	m_pfMix->flush();
	m_pMixLock->unlock();
}

// logErrorMessage
void CBaseInterface::logErrorMessage(const string sErr)
{
	m_pLogLock->lock();
	cout << sErr;
	*(m_pfLog) << sErr;
	m_pfLog->flush();
	m_pLogLock->unlock();
}
