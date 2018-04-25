from util import get_transaction
from pyalgotrade.transaction.barfeed import TransactionLiveFeed
from pyalgotrade.bar import Frequency
from pyalgotrade.transaction.barfeed import Timer


def test_tick():
    timer = Timer(20180423)
    ticks = []

    while True:
        x = timer.next(Frequency.TICK)
        if x is None:
            break
        ticks.append(x)
#    print len(ticks)
    assert(len(ticks)==4802)

    
def test_bar():
    df = get_transaction(open('data/transaction_000001.20180423','rb'),interval=1)
    
    liveFeed = TransactionLiveFeed({'000001':df}, Frequency.TICK, isLive=False)
    liveFeed.start()

    s = 0

    while not liveFeed.eof():
        bars = liveFeed.getNextBars()        
        if bars is not None:
            open_, close, high, low, volume = bars['000001'].getOpen(), bars['000001'].getClose(), bars['000001'].getHigh(), bars['000001'].getLow(), bars['000001'].getVolume()
            s += volume
            assert(open_>=low and open_<=high and close>=low and close<=high and volume>=0)

    print s
    assert(s>=107000000 and s<=108000000)



if __name__ == '__main__':
    test_tick()
    test_bar()









