from dask.distributed import Client
from dask.delayed_lambda import delayed

import random
import time


def f(a):
    def f_prime(b):
        time.sleep(random.randint(2, 5))
        return b ** 2

    return delayed(f_prime)(a)


if __name__ == '__main__':
    client = Client()  # set up local cluster on your laptop

    print delayed(sum)(map(f, range(5))).compute()

    pass
    #time.sleep(60)
