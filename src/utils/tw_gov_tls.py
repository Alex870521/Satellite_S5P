"""TLS for the Taiwanese government APIs, which fail Python 3.13+'s strict check.

Since Python 3.13, ``ssl.create_default_context()`` turns on
``VERIFY_X509_STRICT``, which enforces RFC 5280's requirement that a CA
certificate carry a Subject Key Identifier extension. **TWCA Global Root CA**
— issued in 2010, and the trust anchor for most of Taiwan's government sites —
does not have one. Not the server's copy: the one inside certifi's bundle is
missing it too, so there is nothing an updated CA bundle can fix.

The result, on 3.14:

    SSLCertVerificationError: certificate verify failed:
    Missing Subject Key Identifier

Taiwanese government sites behind that root that fail this way include:

    data.moenv.gov.tw      環境部開放資料
    codis.cwa.gov.tw       CODIS 氣象觀測
    opendata.cwa.gov.tw    中央氣象署開放資料
    www.cwa.gov.tw         中央氣象署

What this module does NOT do is disable verification. ``VERIFY_X509_STRICT``
governs *formal* conformance of the chain — whether extensions that RFC 5280
mandates are present. Clearing it leaves every check that decides whether the
connection is safe: signature validation up the chain, validity dates, basic
constraints, and hostname matching. A forged certificate is rejected exactly as
before; what is now accepted is a genuine one whose CA predates the rule.

The relaxation is also per-host. ``requests_session()`` mounts the adapter only
on the four prefixes above, so a connection to anywhere else in the same
session keeps the strict default — including a redirect that leaves the
government host.

Remove this when TWCA reissues that root with a SKID, or when the sites move to
one of their newer roots (TWCA Root Certification Authority and TWCA CYBER Root
CA both have the extension). Re-check with::

    python -c "import ssl,socket; ctx=ssl.create_default_context(); \\
      socket.create_connection(('data.moenv.gov.tw',443)) and None"
"""
from __future__ import annotations

import ssl

# The hosts known to sit behind TWCA Global Root CA. Deliberately an explicit
# list rather than a "*.gov.tw" rule: the point is to relax as little as
# possible, and a new government host should be added here consciously, after
# checking it actually needs it.
TW_GOV_HOSTS = (
    "data.moenv.gov.tw",
    "codis.cwa.gov.tw",
    "opendata.cwa.gov.tw",
    "www.cwa.gov.tw",
)


def ssl_context() -> ssl.SSLContext:
    """A default context with only the RFC-conformance check cleared."""
    ctx = ssl.create_default_context()
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    return ctx


def requests_session(session=None):
    """A ``requests`` session that can reach the hosts above.

    Pass an existing session to adapt it, or omit to get a new one. Only the
    listed prefixes are mounted, so everything else keeps requests' defaults.
    """
    import requests
    from requests.adapters import HTTPAdapter

    class _TWGovAdapter(HTTPAdapter):
        def init_poolmanager(self, *args, **kwargs):
            kwargs["ssl_context"] = ssl_context()
            return super().init_poolmanager(*args, **kwargs)

        def proxy_manager_for(self, *args, **kwargs):
            kwargs["ssl_context"] = ssl_context()
            return super().proxy_manager_for(*args, **kwargs)

    session = session or requests.Session()
    adapter = _TWGovAdapter()
    for host in TW_GOV_HOSTS:
        session.mount(f"https://{host}", adapter)
    return session
