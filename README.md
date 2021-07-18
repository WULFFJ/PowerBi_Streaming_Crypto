# PowerBi_Streaming_Crypto
PowerBi streaming Crypto API

This is a Python Script to stream data from Crypto Compare's API to PowerBI.
MariaDB or MYSQL was used as an intermediary system of storage and to push the API to PowerBI.

You will have to see PowerBIs developer documentation on their extremely picky limits for this type of application.

When I had this running, I used the scheduler in my Raspberry PI to run this script.

*Required an Apache Server Setup on the Raspberry PI
*MariaDB was installed
*Installation of a VPN tunnel for security
*A port needs to be opened up on your router for specific types of traffic

*CONSIDER LOOKING INTO ALL THE SAFETY CONCERNS BEFORE DOING THIS*

PowerBI does offer a means to do schedule refreshes, but I would recommend installing SQL Server Express for the server processes.  The gateway they provide does not work with Linux.

The difficulty in this script is keeping this running constantly and updated.  If you are not constantly having the PowerBi server updated, you have to reload the server you are storing and re-send everything again.  It was almost like starting over with having to also change several settings on the file.
