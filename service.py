# -*- coding: utf-8 -*-
"""Cloud Charge

Public methods:
lambdafn_reset --> Reset an account's balance
lambdafn_apply_charges --> Apply charges to an account
"""

import os

from redis import Redis
from pottery import Redlock

DEFAULT_RESET_AMOUNT = 10
EXPIRATION_SECONDS = 30 * 24 * 3600  # 30 days


redis_client = None


def _get_redis_client():
    """Create redis client object"""
    global redis_client
    if redis_client is not None:
        return redis_client
    redis_url = os.environ.get("ENDPOINT", "localhost")
    redis_port = int(os.environ.get("PORT", 6379))
    redis_client = Redis.from_url(f'redis://{redis_url}:{redis_port}')
    return redis_client


def _do_locked_operation(event,
                         context,
                         client, key, release_time, op_func,
                         *args, **kwargs):
    """run op_func after locking `key` for `release_time`"""

    result = None
    with Redlock(key=key,
                 masters={client},
                 auto_release_time=release_time):
        result = op_func(event, context, client, *args, **kwargs)
    return result


def _get_account_id(event):
    """Get account id from event"""
    return event.get('account_id')


def _get_balance_key(event):
    """Get balance key from event"""

    return f"balance:{_get_account_id(event)}"


def _get_charges(event):
    """Get balance key from event"""

    return int(event.get('unit'))


def _account_balance_operation(event, context, op_func, *args, **kwargs):
    """Do an account balance operation `op_func`"""

    client = _get_redis_client()
    release_time = context.get_remaining_time_in_millis()
    return _do_locked_operation(event,
                                context,
                                client,
                                _get_balance_key(event),
                                release_time,
                                op_func,
                                *args, **kwargs)


def _reset_balance_func(event,
                        context,
                        client,
                        *args, **kwargs):
    """Reset balance op_func"""

    balance_key = _get_balance_key(event)
    client.set(balance_key, DEFAULT_RESET_AMOUNT,
               ex=EXPIRATION_SECONDS)
    return {"account_id": _get_account_id(event),
            "unit": DEFAULT_RESET_AMOUNT}


def _apply_charges_func(event,
                        context,
                        client,
                        *args, **kwargs):
    """Apply charges op_func"""

    balance_key = _get_balance_key(event)
    charge = _get_charges(event)

    balance = client.get(balance_key)
    if balance >= charge:
        balance -= charge
        client.set(balance_key, balance, keepttl=True)
        result = {"accepted": True}

    result["current_balance"] = balance
    return result


def lambdafn_reset(event, context):
    """Reset balance lambda function"""

    return _account_balance_operation(event, context,
                                      _reset_balance_func)


def lambdafn_apply_charges(event, context):
    """Apply charges lambda function"""

    return _account_balance_operation(event, context,
                                      _apply_charges_func)


def handler(event, context):
    return lambdafn_reset(event, context)
