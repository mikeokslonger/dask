
from dask.delayed_lambda import delayed
from dask.distributed import Client
import dask
import random
import time


def test1(_delayed):
    def f(a):
        print 'running'
        time.sleep(random.randint(1, 3))
        return a ** 2

    print 'Creating DAG'
    DAG =_delayed(f)(5)

    print 'Computing'
    return DAG.compute()


def test2(_delayed):
    def f(a):
        def f_prime(b):
            print 'running'
            time.sleep(random.randint(1, 3))
            return b ** 2

        return _delayed(f_prime)(a)

    def my_sum(inputs):
        print 'inputs to sum: {}'.format(inputs)
        return sum(inputs)

    def f2(a):
        return _delayed(my_sum)(map(f, range(a)))

    print 'Creating DAG'
    DAG = f2(20)

    print 'Computing'
    return DAG.compute()


if __name__ == '__main__':
    print 'Creating Client'

    def test_original():
        dask.distributed.Client()
        print test1(dask.delayed)
        print test2(dask.delayed)

    def test_new():
        dask.distributed.client_lambda.ClientLambda(n_workers=1, threads_per_worker=100)
        # print test1(dask.delayed_lambda.delayed)
        print test2(dask.delayed_lambda.delayed)

    #test_original()
    test_new()