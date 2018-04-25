import Queue
import datetime
import threading
import time
import pytz


import pyalgotrade.logger
from pyalgotrade import bar
from pyalgotrade import dataseries
from pyalgotrade import barfeed
from pyalgotrade import resamplebase
from pyalgotrade.utils import dt
from pyalgotrade.bar import Frequency

logger = pyalgotrade.logger.getLogger("Transaction")


#def to_market_datetime(dateTime):
#    timezone = pytz.timezone('Asia/Shanghai')
#    return dt.localize(dateTime, timezone)


class Timer(object):
    def __init__(self, date):
        self.__open = datetime.datetime(date/10000,date%10000/100,date%100,9,0,0)
        self.__time = self.__open + datetime.timedelta(minutes=30)
        self.__noon = self.__time + datetime.timedelta(hours=2)
        self.__afternoon = self.__noon + datetime.timedelta(minutes=90)
        self.__close = self.__afternoon + datetime.timedelta(hours=2)

    def getMarketOpen(self):
        return self.__open

    def next(self, frequency):
        if self.__time is None:
            return None
        ret = self.__time
        self.update(frequency)
        return ret

    def update(self, frequency):
        self.__time += datetime.timedelta(seconds=frequency)
        if self.__time > self.__noon and self.__time < self.__afternoon:
            self.__time += datetime.timedelta(minutes=90)
        if self.__time > self.__close:
            self.__time = None



class TransactionBar(object):
    def __init__(self, dataSources):
        self.__dfs = dataSources
        self.__lastQuoteTime = {}

    def get(self, identifier, endTime, method='co'):
        startTime = datetime.time(0,0,0)
        if identifier in self.__lastQuoteTime:
            startTime = self.__lastQuoteTime[identifier]

        if method == 'co':
            df = self.__dfs[identifier]
            sdf = df.ix[(df.Time>=startTime)&(df.Time<endTime)]

        bar = None

        if len(sdf):
            open_ = sdf.iloc[0]['nTradePrice']
            high = sdf['nTradePrice'].max()
            low = sdf['nTradePrice'].min()
            close = sdf.iloc[-1]['nTradePrice']
            volume = sdf['nTradeVolume'].sum()           
            bar = bar.BasicBar(endTime, open_, high, low, close, volume, None, Frequency.Tick)

        self.__lastQuoteTime[identifier] = endTime


class TransactionPollingThread(threading.Thread):
    def __init__(self, dataSources, isLive):
        super(TransactionPollingThread, self).__init__()
        self._identifiers = dataSources.keys()        
        self._isLive = isLive

        if isLive:
            raise NotImplementedError()
        else:
            self._tb = TransactionBar(dataSources)

        self._bars = {}

        self.__stopped = False

    def __wait(self):
        nextCall = self.getNextCallDateTime()

        if self.__isLive:
            time_diff = self.nexCall - 0
            time.sleep(time_diff)
        
        for identifier in self._identifiers:
            self._bars[identifier] = self._tb.get(identifier, nextCall)


    def stop(self):
        self.__stopped = True

    def stopped(self):
        return self.__stopped

    def run(self):
        logger.debug("Thread started.")
        while not self.__stopped:
            self.__wait()
            if not self.__stopped:
                try:
                    self.doCall()
                except Exception, e:
                    logger.critical("Unhandled exception", exc_info=e)
        logger.debug("Thread finished.")

    # Must return a non-naive datetime.
    def getNextCallDateTime(self):
        raise NotImplementedError()

    def doCall(self):
        raise NotImplementedError()


class TransactionBarFeedThread(TransactionPollingThread):
    # Events
    ON_BARS = 1

    def __init__(self, queue, dataSources, frequency, isLive):
        super(TransactionBarFeedThread, self).__init__(dataSources, isLive)
        self.__queue = queue
        self.__frequency = frequency
        self.__timer = Timer(dataSources.values[0].iloc[0]['nDate'])
        self.__updateNextBarClose()

    def __updateNextBarClose(self):
        self.__nextBarClose = self.__timer.next(self.__frequency)
        if self.__nextBarClose == None:
            self.stop()        

    def getNextCallDateTime(self):
        return self.__nextBarClose

    def doCall(self):
        self.__updateNextBarClose()

        if len(self._bars):
            bars = bar.Bars(self._bars)
            self.__queue.put((TransactionBarFeedThread.ON_BARS, bars))


class TransactionLiveFeed(barfeed.BaseBarFeed):
    QUEUE_TIMEOUT = 0.01

    def __init__(self, dataSources, frequency, maxLen=dataseries.DEFAULT_MAX_LEN, isLive=True):
        barfeed.BaseBarFeed.__init__(self, frequency, maxLen)

        self.__identifiers = dataSources.keys()
        self.__frequency = frequency
        self.__queue = Queue.Queue()

        self.__thread = TransactionBarFeedThread(self.__queue, dataSources, frequency, isLive)
        for instrument in self.__identifiers:
            self.registerInstrument(instrument)

    ######################################################################
    # observer.Subject interface
    def start(self):
        if self.__thread.is_alive():
            raise Exception("Already strated")

        # Start the thread that runs the client.
        self.__thread.start()

    def stop(self):
        self.__thread.stop()

    def join(self):
        if self.__thread.is_alive():
            self.__thread.join()

    def eof(self):
        return self.__thread.stopped()

    def peekDateTime(self):
        return None

    ######################################################################
    # barfeed.BaseBarFeed interface
    def getCurrentDateTime(self):
        return self.getNextCallDateTime()

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        ret = None
        try:
            eventType, eventData = self.__queue.get(True, TransactionLiveFeed.QUEUE_TIMEOUT)
            if eventType == TransactionBarFeedThread.ON_BARS:
                ret = eventData
            else:
                logger.error("Invalid event received: %s - %s" % (eventType, eventData))
        except Queue.Empty:
            pass
        return ret



if __name__ == '__main__':
    from util import get_transaction
    df = get_transaction(open('/home/disco/data/whdata/tony/20180423/transaction_000001.20180423','rb'),interval=1)
    print df.iloc[:10]
    exit()
    
    liveFeed = TransactionLiveFeed({'000001':df}, Frequency.TICK, isLive=False)
    liveFeed.start()

    while not liveFeed.eof():
        bars = liveFeed.getNextBars()
        if bars is not None:
            print bars['000001'].getHigh(), bars['000001'].getDateTime()
            # test/












