from pyalgotrade.transaction.barfeed import Timer
from pyalgotrade.bar import Frequency


def test_tick():
    timer = Timer(20180423)
    ticks = []

    while True:
        x = timer.next(Frequency.TICK)
        if x is None:
            break
        ticks.append(x)
#    print len(ticks)
    assert(len(ticks)==4801)
