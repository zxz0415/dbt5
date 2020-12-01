#include "DM.h"

#include "Driver.h"
#include "Customer.h"
//#include <unistd.h>

// global variables
pthread_t *g_tid = NULL;
int stop_time = 0;
int start_time=0;

// Constructor
CDriver::CDriver(char *szInDir,
		TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
		INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 iSeed,
		char *szBHaddr, int iBHlistenPort, int iUsers, int iPacingDelay,
		char *outputDirectory, int scene)
{
	char filename[iMaxPath + 1];
	snprintf(filename, iMaxPath, "%s/Driver.log", outputDirectory);
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, filename, &m_fmt);
	m_pDriverCETxnSettings = new TDriverCETxnSettings;
	m_InputFiles.Initialize(eDriverEGenLoader, iConfiguredCustomerCount,
			iActiveCustomerCount, szInDir);

	snprintf(filename, iMaxPath, "%s/Driver_Error.log", outputDirectory);
	m_fLog.open(filename, ios::out);
	snprintf(filename, iMaxPath, "%s/%s", outputDirectory, CE_MIX_LOG_NAME);
	m_fMix.open(filename, ios::out);

	strncpy(this->szInDir, szInDir, iMaxPath);
	this->szInDir[iMaxPath] = '\0';
	this->iConfiguredCustomerCount = iConfiguredCustomerCount;
	this->iActiveCustomerCount = iActiveCustomerCount;
	this->iScaleFactor = iScaleFactor;
	this->iDaysOfInitialTrades = iDaysOfInitialTrades;
	this->iSeed = iSeed;
	strncpy(this->szBHaddr, szBHaddr, iMaxHostname);
	this->szBHaddr[iMaxHostname] = '\0';
	this->iBHlistenPort = iBHlistenPort;
	this->iUsers = iUsers;
	this->scene=scene;
	this->iPacingDelay = iPacingDelay;
	strncpy(this->outputDirectory, outputDirectory, iMaxPath);
	this->outputDirectory[iMaxPath] = '\0';
	//
	// initialize DMSUT interface
	m_pCDMSUT = new CDMSUT(szBHaddr, iBHlistenPort, &m_fLog, &m_fMix,
			&m_LogLock, &m_MixLock);

	// initialize DM - Data Maintenance
	if (iSeed == 0) {
		m_pCDM = new CDM(m_pCDMSUT, m_pLog, m_InputFiles,
				iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
				iDaysOfInitialTrades, pthread_self());
	} else {
		// Specifying the random number generator seed is considered an
		// invalid run.
		m_pCDM = new CDM(m_pCDMSUT, m_pLog, m_InputFiles,
				iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
				iDaysOfInitialTrades, pthread_self(), iSeed);
	}
}

void *customerWorkerThread(void *data)
{
	CCustomer *customer;
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);

	ostringstream osErr;
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (pThrParam->pDriver->iPacingDelay / 1000);
	ts.tv_nsec = (long) (pThrParam->pDriver->iPacingDelay % 1000) *
			1000000;

	customer = new CCustomer(pThrParam->pDriver->szInDir,
			pThrParam->pDriver->iConfiguredCustomerCount,
			pThrParam->pDriver->iActiveCustomerCount,
			pThrParam->pDriver->iScaleFactor,
			pThrParam->pDriver->iDaysOfInitialTrades,
			pThrParam->pDriver->iSeed,
			pThrParam->pDriver->szBHaddr,
			pThrParam->pDriver->iBHlistenPort,
			pThrParam->pDriver->iUsers,
			pThrParam->pDriver->iPacingDelay,
			pThrParam->pDriver->outputDirectory,
			&pThrParam->pDriver->m_fMix,
			&pThrParam->pDriver->m_MixLock);
	do {
		customer->DoTxn();

		// wait for pacing delay -- this delays happens after the mix logging
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				osErr << "pacing delay time invalid " << ts.tv_sec << " s "
						<< ts.tv_nsec << " ns" << endl;
				pThrParam->pDriver->logErrorMessage(osErr.str());
				break;
			}
		}
	} while (time(NULL) < stop_time);

	osErr << "User thread # " << pthread_self() << " terminated." << endl;
	pThrParam->pDriver->logErrorMessage(osErr.str());
	//pThrParam->pDriver->m_MixLock.lock();
	//pThrParam->pDriver->m_fMix << (int) time(NULL) << ",TERMINATED,,," <<
	//		(long long) pthread_self() << endl;
	//pThrParam->pDriver->m_MixLock.unlock();

	delete pThrParam;
	return NULL;
}

void *customerWorkerThread2(void *data)
{
	CCustomer *customer;
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);

	ostringstream osErr;
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (pThrParam->pDriver->iPacingDelay / 1000);
	ts.tv_nsec = (long) (pThrParam->pDriver->iPacingDelay % 1000) *
			1000000;

	customer = new CCustomer(pThrParam->pDriver->szInDir,
			pThrParam->pDriver->iConfiguredCustomerCount,
			pThrParam->pDriver->iActiveCustomerCount,
			pThrParam->pDriver->iScaleFactor,
			pThrParam->pDriver->iDaysOfInitialTrades,
			pThrParam->pDriver->iSeed,
			pThrParam->pDriver->szBHaddr,
			pThrParam->pDriver->iBHlistenPort,
			pThrParam->pDriver->iUsers,
			pThrParam->pDriver->iPacingDelay,
			pThrParam->pDriver->outputDirectory,
			&pThrParam->pDriver->m_fMix,
			&pThrParam->pDriver->m_MixLock);
	do {
		customer->DoTxn2();

		// wait for pacing delay -- this delays happens after the mix logging
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				osErr << "pacing delay time invalid " << ts.tv_sec << " s "
						<< ts.tv_nsec << " ns" << endl;
				pThrParam->pDriver->logErrorMessage(osErr.str());
				break;
			}
		}
	} while (time(NULL) < stop_time);

	osErr << "User thread # " << pthread_self() << " terminated." << endl;
	pThrParam->pDriver->logErrorMessage(osErr.str());
	//pThrParam->pDriver->m_MixLock.lock();
	//pThrParam->pDriver->m_fMix << (int) time(NULL) << ",TERMINATED,,," <<
	//		(long long) pthread_self() << endl;
	//pThrParam->pDriver->m_MixLock.unlock();

	delete pThrParam;
	return NULL;
}

void *customerWorkerThread3(void *data)
{
	CCustomer *customer;
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);

	ostringstream osErr;
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (pThrParam->pDriver->iPacingDelay / 1000);
	ts.tv_nsec = (long) (pThrParam->pDriver->iPacingDelay % 1000) *
			1000000;

	customer = new CCustomer(pThrParam->pDriver->szInDir,
			pThrParam->pDriver->iConfiguredCustomerCount,
			pThrParam->pDriver->iActiveCustomerCount,
			pThrParam->pDriver->iScaleFactor,
			pThrParam->pDriver->iDaysOfInitialTrades,
			pThrParam->pDriver->iSeed,
			pThrParam->pDriver->szBHaddr,
			pThrParam->pDriver->iBHlistenPort,
			pThrParam->pDriver->iUsers,
			pThrParam->pDriver->iPacingDelay,
			pThrParam->pDriver->outputDirectory,
			&pThrParam->pDriver->m_fMix,
			&pThrParam->pDriver->m_MixLock);
	do {
		customer->DoTxn3();

		// wait for pacing delay -- this delays happens after the mix logging
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				osErr << "pacing delay time invalid " << ts.tv_sec << " s "
						<< ts.tv_nsec << " ns" << endl;
				pThrParam->pDriver->logErrorMessage(osErr.str());
				break;
			}
		}
	} while (time(NULL) < stop_time);

	osErr << "User thread # " << pthread_self() << " terminated." << endl;
	pThrParam->pDriver->logErrorMessage(osErr.str());
	//pThrParam->pDriver->m_MixLock.lock();
	//pThrParam->pDriver->m_fMix << (int) time(NULL) << ",TERMINATED,,," <<
	//		(long long) pthread_self() << endl;
	//pThrParam->pDriver->m_MixLock.unlock();

	delete pThrParam;
	return NULL;
}

void *customerWorkerThread4(void *data)
{
	CCustomer *customer;
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);

	ostringstream osErr;
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (pThrParam->pDriver->iPacingDelay / 1000);
	ts.tv_nsec = (long) (pThrParam->pDriver->iPacingDelay % 1000) *
			1000000;

	customer = new CCustomer(pThrParam->pDriver->szInDir,
			pThrParam->pDriver->iConfiguredCustomerCount,
			pThrParam->pDriver->iActiveCustomerCount,
			pThrParam->pDriver->iScaleFactor,
			pThrParam->pDriver->iDaysOfInitialTrades,
			pThrParam->pDriver->iSeed,
			pThrParam->pDriver->szBHaddr,
			pThrParam->pDriver->iBHlistenPort,
			pThrParam->pDriver->iUsers,
			pThrParam->pDriver->iPacingDelay,
			pThrParam->pDriver->outputDirectory,
			&pThrParam->pDriver->m_fMix,
			&pThrParam->pDriver->m_MixLock);
	do {
		customer->DoTxn4();
		// wait for pacing delay -- this delays happens after the mix logging
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				osErr << "pacing delay time invalid " << ts.tv_sec << " s "
						<< ts.tv_nsec << " ns" << endl;
				pThrParam->pDriver->logErrorMessage(osErr.str());
				break;
			}
		}
	} while (time(NULL) < stop_time);

	osErr << "User thread # " << pthread_self() << " terminated." << endl;
	pThrParam->pDriver->logErrorMessage(osErr.str());
	//pThrParam->pDriver->m_MixLock.lock();
	//pThrParam->pDriver->m_fMix << (int) time(NULL) << ",TERMINATED,,," <<
	//		(long long) pthread_self() << endl;
	//pThrParam->pDriver->m_MixLock.unlock();

	delete pThrParam;
	return NULL;
}

void *customerWorkerThread5(void *data)
{
	CCustomer *customer;
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);

	ostringstream osErr;
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (pThrParam->pDriver->iPacingDelay / 1000);
	ts.tv_nsec = (long) (pThrParam->pDriver->iPacingDelay % 1000) *
			1000000;

	customer = new CCustomer(pThrParam->pDriver->szInDir,
			pThrParam->pDriver->iConfiguredCustomerCount,
			pThrParam->pDriver->iActiveCustomerCount,
			pThrParam->pDriver->iScaleFactor,
			pThrParam->pDriver->iDaysOfInitialTrades,
			pThrParam->pDriver->iSeed,
			pThrParam->pDriver->szBHaddr,
			pThrParam->pDriver->iBHlistenPort,
			pThrParam->pDriver->iUsers,
			pThrParam->pDriver->iPacingDelay,
			pThrParam->pDriver->outputDirectory,
			&pThrParam->pDriver->m_fMix,
			&pThrParam->pDriver->m_MixLock);
	do {
		customer->DoTxn5();
		// wait for pacing delay -- this delays happens after the mix logging
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				osErr << "pacing delay time invalid " << ts.tv_sec << " s "
						<< ts.tv_nsec << " ns" << endl;
				pThrParam->pDriver->logErrorMessage(osErr.str());
				break;
			}
		}
	} while (time(NULL) < stop_time);

	osErr << "User thread # " << pthread_self() << " terminated." << endl;
	pThrParam->pDriver->logErrorMessage(osErr.str());
	//pThrParam->pDriver->m_MixLock.lock();
	//pThrParam->pDriver->m_fMix << (int) time(NULL) << ",TERMINATED,,," <<
	//		(long long) pthread_self() << endl;
	//pThrParam->pDriver->m_MixLock.unlock();

	delete pThrParam;
	return NULL;
}

// entry point for worker thread
void entryCustomerWorkerThread(void* data, int iThrNumber)
{
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}

		// create the thread in the joinable state
		status = pthread_create(&g_tid[iThrNumber], &threadAttribute,
				&customerWorkerThread, data);

		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	} catch(CThreadErr *pErr) {
		ostringstream osErr;
		osErr << "Thread " << iThrNumber << " didn't spawn correctly" << endl <<
				endl << "Error: " << pErr->ErrorText() <<
				" at EntryCustomerWorkerThread" << endl;
		pThrParam->pDriver->logErrorMessage(osErr.str());
		delete pErr;
	}
}

void entryCustomerWorkerThread2(void* data, int iThrNumber)
{
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}

		// create the thread in the joinable state
		status = pthread_create(&g_tid[iThrNumber], &threadAttribute,
				&customerWorkerThread2, data);

		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	} catch(CThreadErr *pErr) {
		ostringstream osErr;
		osErr << "Thread " << iThrNumber << " didn't spawn correctly" << endl <<
				endl << "Error: " << pErr->ErrorText() <<
				" at EntryCustomerWorkerThread" << endl;
		pThrParam->pDriver->logErrorMessage(osErr.str());
		delete pErr;
	}
}

void entryCustomerWorkerThread3(void* data, int iThrNumber)
{
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}

		// create the thread in the joinable state
		status = pthread_create(&g_tid[iThrNumber], &threadAttribute,
				&customerWorkerThread3, data);

		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	} catch(CThreadErr *pErr) {
		ostringstream osErr;
		osErr << "Thread " << iThrNumber << " didn't spawn correctly" << endl <<
				endl << "Error: " << pErr->ErrorText() <<
				" at EntryCustomerWorkerThread" << endl;
		pThrParam->pDriver->logErrorMessage(osErr.str());
		delete pErr;
	}
}


void entryCustomerWorkerThread4(void* data, int iThrNumber){
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}

		// create the thread in the joinable state
		status = pthread_create(&g_tid[iThrNumber], &threadAttribute,
				&customerWorkerThread4, data);

		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	} catch(CThreadErr *pErr) {
		ostringstream osErr;
		osErr << "Thread " << iThrNumber << " didn't spawn correctly" << endl <<
				endl << "Error: " << pErr->ErrorText() <<
				" at EntryCustomerWorkerThread" << endl;
		pThrParam->pDriver->logErrorMessage(osErr.str());
		delete pErr;
	}
}

void entryCustomerWorkerThread5(void* data, int iThrNumber){
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}
		// create the thread in the joinable state
		status = pthread_create(&g_tid[iThrNumber], &threadAttribute,
				&customerWorkerThread5, data);

		if (status != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	} catch(CThreadErr *pErr) {
		ostringstream osErr;
		osErr << "Thread " << iThrNumber << " didn't spawn correctly" << endl <<
				endl << "Error: " << pErr->ErrorText() <<
				" at EntryCustomerWorkerThread" << endl;
		pThrParam->pDriver->logErrorMessage(osErr.str());
		delete pErr;
	}
}


// Destructor
CDriver::~CDriver()
{
	delete m_pCDM;
	delete m_pCDMSUT;

	m_fMix.close();
	m_fLog.close();

	delete m_pDriverCETxnSettings;
	delete m_pLog;
}

// RunTest
void CDriver::runTest(int iSleep, int iTestDuration)
{
	int scene1=this->scene;
	g_tid = (pthread_t*) malloc(sizeof(pthread_t) * iUsers * 100);

	// before starting the test run Trade-Cleanup transaction
	cout << endl <<
			"Running Trade-Cleanup transaction before starting the test..." <<
			endl;
	m_pCDM->DoCleanupTxn();
	cout << "Trade-Cleanup transaction completed." << endl << endl;

	// time to sleep between thread creation, convert from millaseconds to
	// nanoseconds.
	struct timespec ts, rem;
	ts.tv_sec = (time_t) (iSleep / 1000);
	ts.tv_nsec = (long) (iSleep % 1000) * 1000000;

	// Caulculate when the test should stop.
	int threads_start_time =
			(int) ((double) iSleep / 1000.0 * (double) iUsers);
	start_time = time(NULL);
	stop_time = time(NULL) + iTestDuration + threads_start_time;

	CDateTime dtAux;
	dtAux.SetToCurrent();

	cout << "Test is starting at " << dtAux.ToStr(02) << endl <<
			"Estimated duration of ramp-up: " << threads_start_time <<
			" seconds" << endl;

	dtAux.AddMinutes((iTestDuration + threads_start_time)/60);
	cout << "Estimated end time " << dtAux.ToStr(02) << endl;

	logErrorMessage(">> Start of ramp-up.\n");

	// start thread that runs the Data Maintenance transaction
	entryDMWorkerThread(this);

	// parameter for the new thread
	PCustomerThreadParam pThrParam;

	if(scene1==1 || scene1==6 || scene1==7){
	for (int i = 1; i <= iUsers; i++) {
		pThrParam = new TCustomerThreadParam;
		// zero the structure
		memset(pThrParam, 0, sizeof(TCustomerThreadParam));
		pThrParam->pDriver = this;
		entryCustomerWorkerThread(reinterpret_cast<void *>(pThrParam), i);
		// Sleep for between starting terminals
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				ostringstream osErr;
				osErr << "sleep time invalid " << ts.tv_sec << " s " <<
						ts.tv_nsec << " ns" << endl;
				logErrorMessage(osErr.str());
				break;
			}
		}
	}
	//m_MixLock.lock();
	//m_fMix << (int) time(NULL) << ",START,,," <<
	//		(long long) pthread_self() << endl;
	//m_MixLock.unlock();
	logErrorMessage(">> End of ramp-up.\n\n");
	for (int i = 0; i <= iUsers; i++) {
		if (pthread_join(g_tid[i], NULL) != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
					"Driver::RunTest" );
		}
	}
}

	if(scene1==2){
	for (int i = 1; i <= iUsers; i++) {
		pThrParam = new TCustomerThreadParam;
		// zero the structure
		memset(pThrParam, 0, sizeof(TCustomerThreadParam));
		pThrParam->pDriver = this;
		entryCustomerWorkerThread2(reinterpret_cast<void *>(pThrParam), i);

		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				ostringstream osErr;
				osErr << "sleep time invalid " << ts.tv_sec << " s " <<
						ts.tv_nsec << " ns" << endl;
				logErrorMessage(osErr.str());
				break;
			}
		}
	}
	//m_MixLock.lock();
	//m_fMix << (int) time(NULL) << ",START,,," <<
	//		(long long) pthread_self() << endl;
	//m_MixLock.unlock();
	logErrorMessage(">> End of ramp-up.\n\n");
	for (int i = 0; i <= iUsers; i++) {
		if (pthread_join(g_tid[i], NULL) != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
					"Driver::RunTest" );
		}
	}
}

	if(scene1==3){
		for (int i = 1; i <= iUsers; i++) {
			pThrParam = new TCustomerThreadParam;
			// zero the structure
			memset(pThrParam, 0, sizeof(TCustomerThreadParam));
			pThrParam->pDriver = this;
			entryCustomerWorkerThread3(reinterpret_cast<void *>(pThrParam), i);

			while (nanosleep(&ts, &rem) == -1) {
				if (errno == EINTR) {
					memcpy(&ts, &rem, sizeof(timespec));
				} else {
					ostringstream osErr;
					osErr << "sleep time invalid " << ts.tv_sec << " s " <<
							ts.tv_nsec << " ns" << endl;
					logErrorMessage(osErr.str());
					break;
				}
			}
		}
		//m_MixLock.lock();
		//m_fMix << (int) time(NULL) << ",START,,," <<
		//		(long long) pthread_self() << endl;
		//m_MixLock.unlock();
		logErrorMessage(">> End of ramp-up.\n\n");
		for (int i = 0; i <= iUsers; i++) {
			if (pthread_join(g_tid[i], NULL) != 0) {
				throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
						"Driver::RunTest" );
			}
		}
	}
	if(scene1==4){
	for (int i = 1; i <= iUsers; i++) {
		pThrParam = new TCustomerThreadParam;
		// zero the structure
		memset(pThrParam, 0, sizeof(TCustomerThreadParam));
		pThrParam->pDriver = this;
		entryCustomerWorkerThread4(reinterpret_cast<void *>(pThrParam), i);

		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				ostringstream osErr;
				osErr << "sleep time invalid " << ts.tv_sec << " s " <<
						ts.tv_nsec << " ns" << endl;
				logErrorMessage(osErr.str());
				break;
			}
		}
	}
	//m_MixLock.lock();
	//m_fMix << (int) time(NULL) << ",START,,," <<
	//		(long long) pthread_self() << endl;
	//m_MixLock.unlock();
	logErrorMessage(">> End of ramp-up.\n\n");
	for (int i = 0; i <= iUsers; i++) {
		if (pthread_join(g_tid[i], NULL) != 0) {
			throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
					"Driver::RunTest" );
		}
	}
}

	if(scene1==5){
		for (int i = 1; i <= iUsers; i++) {
			pThrParam = new TCustomerThreadParam;
			// zero the structure
			memset(pThrParam, 0, sizeof(TCustomerThreadParam));
			pThrParam->pDriver = this;
			entryCustomerWorkerThread5(reinterpret_cast<void *>(pThrParam), i);

			while (nanosleep(&ts, &rem) == -1) {
				if (errno == EINTR) {
					memcpy(&ts, &rem, sizeof(timespec));
				} else {
					ostringstream osErr;
					osErr << "sleep time invalid " << ts.tv_sec << " s " <<
							ts.tv_nsec << " ns" << endl;
					logErrorMessage(osErr.str());
					break;
				}
			}
		}
		//m_MixLock.lock();
		//m_fMix << (int) time(NULL) << ",START,,," <<
		//		(long long) pthread_self() << endl;
		//m_MixLock.unlock();
		logErrorMessage(">> End of ramp-up.\n\n");
		for (int i = 0; i <= iUsers; i++) {
			if (pthread_join(g_tid[i], NULL) != 0) {
				throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
						"Driver::RunTest" );
			}
		}
	}

	if(scene1==8){
		int flag1=0;
		int flag2=0;
		while(true){
			if(start_time < (start_time+iTestDuration/2) && flag1==0)
			{
				for (int i = 1; i <= iUsers; i++) {
					pThrParam = new TCustomerThreadParam;
					// zero the structure
					memset(pThrParam, 0, sizeof(TCustomerThreadParam));
					pThrParam->pDriver = this;
					entryCustomerWorkerThread(reinterpret_cast<void *>(pThrParam), i);

					while (nanosleep(&ts, &rem) == -1) {
						if (errno == EINTR) {
							memcpy(&ts, &rem, sizeof(timespec));
						} else {
							ostringstream osErr;
							osErr << "sleep time invalid " << ts.tv_sec << " s " <<
									ts.tv_nsec << " ns" << endl;
							logErrorMessage(osErr.str());
							break;
						}
					}
				}
				flag1=1;
				/*for (int i = 0; i <= iUsers; i++) {
					if (pthread_join(g_tid[i], NULL) != 0) {
						throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
								"Driver::RunTest" );
					}
				}*/
			}
			else if(time(NULL) >= (start_time+iTestDuration/2) && flag2==0)
			{
				for (int i = (iUsers+1); i <= iUsers*10; i++) {
					pThrParam = new TCustomerThreadParam;
					// zero the structure
					memset(pThrParam, 0, sizeof(TCustomerThreadParam));
					pThrParam->pDriver = this;
					entryCustomerWorkerThread(reinterpret_cast<void *>(pThrParam), i);
			}
				//m_MixLock.lock();
				//m_fMix << (int) time(NULL) << ",START,,," << (long long) pthread_self() << endl;
				//m_MixLock.unlock();
				//logErrorMessage(">> End of ramp-up.\n\n");
				flag2=1;
				for (int i = 1; i <= iUsers*10; i++) {
					if (pthread_join(g_tid[i], NULL) != 0) {
						throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
								"Driver::RunTest" );
					}
				}
				break;
			}
		}
}
	if(scene1==9){
		int flag1=0;
		int flag2=0;
		while(true){
			if(start_time < (start_time+iTestDuration/2) && flag1==0)
			{
				for (int i = 1; i <= iUsers; i++) {
					pThrParam = new TCustomerThreadParam;
					// zero the structure
					memset(pThrParam, 0, sizeof(TCustomerThreadParam));
					pThrParam->pDriver = this;
					entryCustomerWorkerThread(reinterpret_cast<void *>(pThrParam), i);

					while (nanosleep(&ts, &rem) == -1) {
						if (errno == EINTR) {
							memcpy(&ts, &rem, sizeof(timespec));
						} else {
							ostringstream osErr;
							osErr << "sleep time invalid " << ts.tv_sec << " s " <<
									ts.tv_nsec << " ns" << endl;
							logErrorMessage(osErr.str());
							break;
						}
					}
				}
				flag1=1;
				/*for (int i = 0; i <= iUsers; i++) {
					if (pthread_join(g_tid[i], NULL) != 0) {
						throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
								"Driver::RunTest" );
					}
				}*/
			}
			else if(time(NULL) >= (start_time+iTestDuration/2) && flag2==0)
			{
				for (int i = 1; i <= 90; i++) {
					usleep(100000);
					pthread_cancel(g_tid[i]);
			}
				//m_MixLock.lock();
				//m_fMix << (int) time(NULL) << ",START,,," << (long long) pthread_self() << endl;
				//m_MixLock.unlock();
				//logErrorMessage(">> End of ramp-up.\n\n");
				flag2=1;
				for (int i = 1; i <= iUsers; i++) {
					if (pthread_join(g_tid[i], NULL) != 0) {
						throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
								"Driver::RunTest" );
					}
				}
				break;
			}
		}
}



}


// DM worker thread
void *dmWorkerThread(void *data)
{
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);

	// The Data-Maintenance transaction must run once per minute.
	// FIXME: What do we do if the transaction takes more than a minute
	// to run?
	time_t start_time;
	time_t end_time;
	unsigned int remaining;
	do {
		start_time = time(NULL);
		pThrParam->pDriver->m_pCDM->DoTxn();
		end_time = time(NULL);
		remaining = 60 - (end_time - start_time);
		if (end_time < stop_time && remaining > 0)
			sleep(remaining);
	} while (end_time < stop_time);

	pThrParam->pDriver->logErrorMessage("Data-Maintenance thread stopped.\n");
	delete pThrParam;

	return NULL;
}

// entry point for worker thread
void entryDMWorkerThread(CDriver *ptr)
{
	PCustomerThreadParam pThrParam = new TCustomerThreadParam;
	memset(pThrParam, 0, sizeof(TCustomerThreadParam)); // zero the structure
	pThrParam->pDriver = ptr;

	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		pthread_attr_init(&threadAttribute);

		// create the thread in the joinable state
		pthread_create(&g_tid[0], &threadAttribute, &dmWorkerThread,
				reinterpret_cast<void *>(pThrParam));

		pThrParam->pDriver->logErrorMessage(
				">> Data-Maintenance thread started.\n");
	} catch(CThreadErr *pErr) {
		ostringstream msg;
		msg << "Data-Maintenance thread not created successfully, exiting..." <<
				endl;
		pThrParam->pDriver->logErrorMessage(msg.str());
		delete pErr;
		exit(1);
	}
}

// logErrorMessage
void CDriver::logErrorMessage(const string sErr)
{
	m_LogLock.lock();
	cout << sErr;
	m_fLog << sErr;
	m_fLog.flush();
	m_LogLock.unlock();
}
