# anet
experiments with asynchronous network message broker with connections pool


`server/` folder contains testing server
Connection Pull implementa message length of `int16` (2 bytes) BigEndian.
Message added with header before the message body which shoudl be returned by server in response for matching.
