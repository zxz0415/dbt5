cmake_minimum_required(VERSION 3.17)
project(dbt5)

set(CMAKE_CXX_STANDARD 14)
#
# Defining binaries so to minimized typoes.
#

#SET(bin/BrokerageHouseMain)
#SET(bin/DriverMain)
#SET(bin/MarketExchangeMain)
#SET(DBT5_TEST_TXN bin/TestTxn)

#
# Check for large file support by using 'getconf'.
#

#FIND_PROGRAM(GETCONF getconf)
#IF (GETCONF)
#    SET(GETCONF_FOUND TRUE)
#ENDIF (GETCONF)
#
#IF (GETCONF_FOUND)
#    EXECUTE_PROCESS(COMMAND ${GETCONF} LFS_CFLAGS COMMAND ${GETCONF}
#            LFS64_CFLAGS OUTPUT_VARIABLE LFS_CFLAGS
#            OUTPUT_STRIP_TRAILING_WHITESPACE)
#ENDIF (GETCONF_FOUND)


#
# Defining commonly used paths.
#

SET(EGENINC assi/inc)
SET(EGENSRC assi/src)

SET(TXN src/transactions)
SET(INTF src/interfaces)

SET(BH src/BrokerageHouse)
SET(CE src/Driver)
SET(ME src/MarketExchange)
aux_source_directory(src/threadPool ThreadPool)
aux_source_directory(src/zthreadPool zThreadPool)
#
# Defining common groups of source files.
#

SET(TXN_SOURCE ${TXN}/DBConnection.cpp
               ${TXN}/TxnDBBase.cpp
               ${TXN}/MarketFeedDB.cpp
               ${TXN}/TradeCleanupDB.cpp
               ${TXN}/DataMaintenanceDB.cpp
               ${TXN}/MarketWatchDB.cpp
               ${TXN}/SecurityDetailDB.cpp
               ${TXN}/BrokerVolumeDB.cpp
               ${TXN}/CustomerPositionDB.cpp
               ${TXN}/TradeUpdateDB.cpp
               ${TXN}/TradeLookupDB.cpp
               ${TXN}/TradeResultDB.cpp
               ${TXN}/TradeOrderDB.cpp
               ${TXN}/TradeStatusDB.cpp)

#
# Files to build.
#

ADD_EXECUTABLE(DBT5_BROKERAGE_HOUSE
                ${ThreadPool}
                ${zThreadPool}
               ${EGENSRC}/DateTime
               ${EGENSRC}/locking.cpp
               ${INTF}/CSocket.cpp
               ${INTF}/TxnHarnessSendToMarket.cpp
               ${TXN_SOURCE}
               ${BH}/BrokerageHouseMain.cpp
               ${BH}/BrokerageHouse.cpp)

ADD_EXECUTABLE(DBT5_DRIVER
               ${CE}/Driver.cpp
               ${CE}/DriverMain.cpp
               src/Customer/Customer.cpp
               ${EGENSRC}/BaseLogger.cpp
               ${EGENSRC}/EGenLogFormatterTab.cpp
               ${EGENSRC}/CETxnMixGenerator.cpp
               ${EGENSRC}/CETxnInputGenerator.cpp
               ${EGENSRC}/ReadRowFunctions.cpp
               ${EGENSRC}/InputFlatFilesStructure.cpp
               ${EGENSRC}/error.cpp
               ${EGENSRC}/AddressTable.cpp
               ${EGENSRC}/EGenVersion.cpp
               ${EGENSRC}/CustomerTable.cpp
               ${EGENSRC}/CustomerSelection.cpp
               ${EGENSRC}/Random.cpp
               ${EGENSRC}/Person.cpp
               ${EGENSRC}/DateTime.cpp
               ${EGENSRC}/CE.cpp
               ${EGENSRC}/DM.cpp
               ${EGENSRC}/locking.cpp
               ${EGENSRC}/MEE.cpp
               ${EGENSRC}/MEETradingFloor.cpp
               ${EGENSRC}/MEETickerTape.cpp
               ${EGENSRC}/MEEPriceBoard.cpp
               ${EGENSRC}/WheelTime.cpp
               ${EGENSRC}/MEESecurity.cpp
               ${INTF}/CESUT.cpp
               ${INTF}/CSocket.cpp
               ${INTF}/DMSUT.cpp
               ${INTF}/BaseInterface.cpp)

ADD_EXECUTABLE(DBT5_MARKET_EXCHANGE
               ${EGENSRC}/ReadRowFunctions.cpp
               ${EGENSRC}/BaseLogger.cpp
               ${EGENSRC}/EGenLogFormatterTab.cpp
               ${EGENSRC}/EGenVersion.cpp
               ${EGENSRC}/error.cpp
               ${INTF}/CSocket.cpp
               ${EGENSRC}/Random.cpp
               ${EGENSRC}/DateTime.cpp
               ${EGENSRC}/WheelTime.cpp
               ${EGENSRC}/MEESecurity.cpp
               ${EGENSRC}/MEEPriceBoard.cpp
               ${EGENSRC}/MEETickerTape.cpp
               ${EGENSRC}/MEETradingFloor.cpp
               ${EGENSRC}/MEE.cpp
               ${EGENSRC}/locking.cpp
               ${EGENSRC}/InputFlatFilesStructure.cpp
               ${INTF}/MEESUT.cpp
               ${INTF}/BaseInterface.cpp
               ${ME}/MarketExchange.cpp
               ${ME}/MarketExchangeMain.cpp)

#ADD_EXECUTABLE(${DBT5_TEST_TXN}
# ${EGENSRC}/ReadRowFunctions.cpp
#                  ${EGENSRC}/BaseLogger.cpp
#                  ${EGENSRC}/EGenLogFormatterTab.cpp
#                  ${EGENSRC}/InputFlatFilesStructure.cpp
#                  ${EGENSRC}/error.cpp
#                  ${EGENSRC}/EGenVersion.cpp
#                  ${EGENSRC}/AddressTable.cpp
#                  ${EGENSRC}/CustomerTable.cpp
#                  ${EGENSRC}/CustomerSelection.cpp
#                  ${EGENSRC}/Random.cpp
#                  ${EGENSRC}/Person.cpp
#                  ${EGENSRC}/DateTime.cpp
#                  ${EGENSRC}/CETxnInputGenerator.cpp
#                  ${EGENSRC}/DM.cpp
#                  ${EGENSRC}/WheelTime.cpp
#                  ${EGENSRC}/MEESecurity.cpp
#                  ${EGENSRC}/MEEPriceBoard.cpp
#                  ${EGENSRC}/MEETickerTape.cpp
#                  ${EGENSRC}/MEETradingFloor.cpp
#                  ${EGENSRC}/MEE.cpp
#                  ${EGENSRC}/locking.cpp
#                  ${INTF}/MEESUTtest.cpp
#                  ${INTF}/DMSUTtest.cpp
#                  ${TXN_SOURCE}
#                  ${INTF}/BaseInterface.cpp
#                  ${INTF}/CESUT.cpp
#                  ${INTF}/CSocket.cpp
#                  ${INTF}/TxnHarnessSendToMarketTest.cpp
#                  ${INTF}/TxnHarnessSendToMarket.cpp
#                  ${BH}/BrokerageHouse.cpp
#                  src/TestTransactions/TestTxn.cpp)

#
# Set compiler flags.
#

SET(CMAKE_CXX_FLAGS "-g -Wall -I${EGENINC} -Isrc/include ${LFS_CFLAGS}")

#
# Set linker flags.

target_link_libraries(DBT5_BROKERAGE_HOUSE odbc pthread)
target_link_libraries(DBT5_DRIVER pthread)
target_link_libraries(DBT5_MARKET_EXCHANGE pthread)

SET_TARGET_PROPERTIES(DBT5_BROKERAGE_HOUSE PROPERTIES LINK_FLAGS
        "-lpthread -lodbc")
SET_TARGET_PROPERTIES(DBT5_DRIVER PROPERTIES LINK_FLAGS "-lpthread")
SET_TARGET_PROPERTIES(DBT5_MARKET_EXCHANGE PROPERTIES LINK_FLAGS "-lpthread")
#SET_TARGET_PROPERTIES(${DBT5_TEST_TXN} PROPERTIES LINK_FLAGS
#        "-lpthread -lodbc")

#
# Install rules.
#

INSTALL(PROGRAMS DBT5_BROKERAGE_HOUSE
                 DBT5_DRIVER
                 DBT5_MARKET_EXCHANGE
#                 ${DBT5_TEST_TXN}
        DESTINATION "/bin")
