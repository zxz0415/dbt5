/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - 2006 Rilson Nascimento
 * - 2010 Mark Wong <markwkm@postgresql.org>
 */

//
// PGSQL Database loader class for DAILY_MARKET table.
//

#ifndef PGSQL_DAILY_MARKET_LOAD_H
#define PGSQL_DAILY_MARKET_LOAD_H

namespace TPCE
{

class CPGSQLDailyMarketLoad : public CPGSQLLoader<DAILY_MARKET_ROW>
{
private:
	CDateTime dm_date;

public:
	CPGSQLDailyMarketLoad(const char *szConnectStr,
			const char *szTable = "daily_market")
			: CPGSQLLoader<DAILY_MARKET_ROW>(szConnectStr, szTable) { };

	// copy to the bound location inside this class first
	virtual void WriteNextRecord(PT next_record) {
		dm_date = next_record->DM_DATE;

		fprintf(p, "%s%c%s%c%.2f%c%.2f%c%.2f%c%" PRId64 "\n",
				dm_date.ToStr(iDateTimeFmt), delimiter,
				next_record->DM_S_SYMB, delimiter,
				next_record->DM_CLOSE, delimiter,
				next_record->DM_HIGH, delimiter,
				next_record->DM_LOW, delimiter,
				next_record->DM_VOL);
		// FIXME: Have blind faith that this row of data was built correctly.
		while (fgetc(p) != EOF) ;
	}
};

} // namespace TPCE

#endif // PGSQL_DAILY_MARKET_LOAD_H
