# minecraft-pi-fast-query
Use computer science to speed up the MCPI getBlock API

This is the Python code for a little project I did to
improve the performance of the Minecraft special edition for Raspbery Pi
MCPI getBlock API when getting data for many blocks at once.
Two different techniques were used, multi-threading and not waiting for socket replies,
to improve performance from an average of 60 blocks/second
to a peak of over 5000 blocks/second.
The technique also applies to the getBlockWithData and getHeight APIs.

To use this, the approximate steps are:
- Get a Raspbery Pi with Raspbian.  Minecraft is included.
- Clone my project: git clone git://github.com/joseph-reynolds/minecraft-pi-fast-query
- Start the Minecraft world
- Run the code using Python 2.7.
  - You may need to adjust the world coordinates in the __main__ body.
  - You may also want to play with the thread_count.
