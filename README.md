# Wobbly

Wobbly provides IVOA UWS database storage as a service.
It allows centralizing the UWS database storage for a variety of applications that use the IVOA UWS framework for async tasks so that the applications do not each have to manage database connections, schema migrations, and the other work required to support a service database.

It was written for the Rubin Science Platform and assumes it will be deployed as part of [Phalanx](https://phalanx.lsst.io/).
Services should normally use it via the [UWS support in the Safir library](https://safir.lsst.io/user-guide/uws/index.html).

Wobbly is an implementation of [SQR-096](https://sqr-096.lsst.io/).
See that tech note for more background and discussion.

Wobbly is developed with [FastAPI](https://fastapi.tiangolo.com) and [Safir](https://safir.lsst.io).
