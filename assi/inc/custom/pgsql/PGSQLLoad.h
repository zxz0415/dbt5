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

#ifndef PGSQLLOAD_H
#define PGSQLLOAD_H

#include <stdio.h>

#include "../../EGenUtilities_stdafx.h"
#include "../../Table_Defs.h"
#include "../../EGenBaseLoader_stdafx.h"

#include "pgloader.h"
#include "PGSQLAccountPermissionLoad.h"
#include "PGSQLAddressLoad.h"
#include "PGSQLBrokerLoad.h"
#include "PGSQLCashTransactionLoad.h"
#include "PGSQLChargeLoad.h"
#include "PGSQLCommissionRateLoad.h"
#include "PGSQLCompanyLoad.h"
#include "PGSQLCompanyCompetitorLoad.h"
#include "PGSQLCustomerLoad.h"
#include "PGSQLCustomerAccountLoad.h"
#include "PGSQLCustomerTaxrateLoad.h"
#include "PGSQLDailyMarketLoad.h"
#include "PGSQLExchangeLoad.h"
#include "PGSQLFinancialLoad.h"
#include "PGSQLHoldingLoad.h"
#include "PGSQLHoldingHistoryLoad.h"
#include "PGSQLHoldingSummaryLoad.h"
#include "PGSQLIndustryLoad.h"
#include "PGSQLLastTradeLoad.h"
#include "PGSQLNewsItemLoad.h"
#include "PGSQLNewsXRefLoad.h"
#include "PGSQLSectorLoad.h"
#include "PGSQLSecurityLoad.h"
#include "PGSQLSettlementLoad.h"
#include "PGSQLStatusTypeLoad.h"
#include "PGSQLTaxrateLoad.h"
#include "PGSQLTradeLoad.h"
#include "PGSQLTradeHistoryLoad.h"
#include "PGSQLTradeRequestLoad.h"
#include "PGSQLTradeTypeLoad.h"
#include "PGSQLWatchItemLoad.h"
#include "PGSQLWatchListLoad.h"
#include "PGSQLZipCodeLoad.h"

#endif // #ifndef PGSQLLOAD_H
