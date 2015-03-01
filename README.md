# sqlutils

A set of Scala functions to help interface with JDBC.

## What's the point of another SQL Utility class?
I found myself doing a project in Scala where I needed to do some JDBC interfacing. Now, there's a great library called [ScalikeJDBC] that lets you do JDBC in Scala. It's extremely powerful, and you should use it if you are doing anything of consequence. I, however, had a few issues:
 - I was working with an existing codebase which bringing another library into would have been troublesome.
 - I was in a bit of a time crunch and did not really have the time to learn how to use the library properly.
 - I was not doing anything complicated; only simple CRUD stuff.

Given these factors, I decided to look for something much more simple and straightforward. All I really wanted was to eliminate the boilerplate of JDBC calls. After doing a quick search, I decided it would be easiest/fastest to just roll my own solution. And here we are. I believe that the code is useful, so I wanted put it out there in case anyone else is in a similar situation to me. Hopefully it will save you some time, and if you're not too familiar with Scala, give you some ideas. There's nothing groundbreaking here, just tight Scala code. Check out the test class for examples on how to use it.

License
----

CC0 Universal

[ScalikeJDBC]:http://scalikejdbc.org/