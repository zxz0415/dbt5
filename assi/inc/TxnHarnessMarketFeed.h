#ifndef TXN_HARNESS_MARKET_FEED_H
#define TXN_HARNESS_MARKET_FEED_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CMarketFeed
{
    CMarketFeedDBInterface*     m_db;
    CSendToMarketInterface*     m_pSendToMarket;

public:
    CMarketFeed(CMarketFeedDBInterface *pDB, CSendToMarketInterface *pSendToMarket)
    : m_db(pDB)
    , m_pSendToMarket(pSendToMarket)
    {
    };

    void DoTxn( PMarketFeedTxnInput pTxnInput, PMarketFeedTxnOutput pTxnOutput )
    {
        // Initialization
        TMarketFeedFrame1Input    Frame1Input;
        TMarketFeedFrame1Output   Frame1Output;

        memset(&Frame1Input, 0, sizeof( Frame1Input ));
        memset(&Frame1Output, 0, sizeof( Frame1Output ));

        TXN_HARNESS_SET_STATUS_SUCCESS;

        // Copy Frame 1 Input
        Frame1Input.StatusAndTradeType = pTxnInput->StatusAndTradeType;
        for (int i=0; i<max_feed_len; i++)
        {
          Frame1Input.Entries[i] = pTxnInput->Entries[i];
        }

        // Execute Frame 1
        m_db->DoMarketFeedFrame1(&Frame1Input, &Frame1Output, m_pSendToMarket);

        // Validate Frame 1 Output
        if (Frame1Output.num_updated < pTxnInput->unique_symbols)
        {
            //TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::MFF1_ERROR1);
        }

        // Copy Frame 1 Output
        pTxnOutput->send_len = Frame1Output.send_len;
    }
};

}   // namespace TPCE

#endif //TXN_HARNESS_MARKET_FEED_H
