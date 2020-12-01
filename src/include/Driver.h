#ifndef DRIVER_H
#define DRIVER_H

#include "EGenLogFormatterTab.h"
#include "EGenLogger.h"
#include "InputFlatFilesStructure.h"
#include "DMSUT.h"
#include "locking.h"

using namespace TPCE;

class CDriver
{
private:
	CLogFormatTab m_fmt;
	CEGenLogger *m_pLog;
	CInputFiles m_InputFiles;
	PDriverCETxnSettings m_pDriverCETxnSettings;
	CMutex m_LogLock;
	ofstream m_fLog; // error log file
	ofstream m_fMix; // mix log file

	void logErrorMessage(const string);

	friend void *customerWorkerThread(void *);
	friend void *customerWorkerThread2(void *);
	friend void *customerWorkerThread3(void *);
	friend void *customerWorkerThread4(void *);
	friend void *customerWorkerThread5(void *);
	//friend void *customerWorkerThread9(void *);
	// entry point for driver worker thread
	friend void entryCustomerWorkerThread(void *, int);
	friend void entryCustomerWorkerThread2(void *, int);
	friend void entryCustomerWorkerThread3(void *, int);
	friend void entryCustomerWorkerThread4(void *, int);
	friend void entryCustomerWorkerThread5(void *, int);
	friend void *dmWorkerThread(void *);
	friend void entryDMWorkerThread(CDriver *);

public:
	char szInDir[iMaxPath + 1];
	TIdent iConfiguredCustomerCount;
	TIdent iActiveCustomerCount;
	INT32 iScaleFactor;
	INT32 iDaysOfInitialTrades;
	UINT32 iSeed;
	char szBHaddr[iMaxHostname + 1];
	int iBHlistenPort;
	int iUsers;
	int iPacingDelay;
	int scene;
	char outputDirectory[iMaxPath + 1];
	CMutex m_MixLock;
	CDMSUT *m_pCDMSUT;
	CDM *m_pCDM;

	CDriver(char *, TIdent, TIdent, INT32, INT32, UINT32, char *, int, int,
			int, char *, int);
	~CDriver();

	void runTest(int, int);
};

//parameter structure for the threads
typedef struct TCustomerThreadParam
{
	CDriver *pDriver;
} *PCustomerThreadParam;


#endif // DRIVER_H
