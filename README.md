<div align="center">

### The objective of this project is to simulate file sharing using the BitTorrent protocol, implemented with MPI for distributed communication.

## MPI Communication Handling

* Different tags were used for MPI functions to avoid rare cases where two consecutive send operations (one sending a vector’s length and the next sending the vector itself) could be received in the wrong order

* While this issue only occurred in about 5-6 runs out of 500, using unique tags for each operation ensures consistent results across all executions.


## Clients

</div>

* Initialization Stage: each client reads data from its assigned files and creates two data structures:
  * A mapping between file names and their corresponding hashes.
  * A list of files the client wants to download.
 
* Clients wait until all other clients have sent their file names and hash information to the tracker. Afterward, the downloading process begins.

* Three threads are started: upload, download, and sendConnection:
  * Upload: receives a file request, checks if the client has the requested file as a seed, and responds with either an ACK or a NACK.
  * Download:
      * To follow the torrent protocol, the client first requests file hashes from the tracker (which, in a real scenario, would be used for data validation).
      * Every 10 hashes (including the first request), the client asks the tracker for a list of other clients who have the file.
      * To balance network load, each client tracks its number of uploads. When selecting a peer to download from, the client always prioritizes those with fewer uploads (this is managed by the sendConnection thread).
      * The received list of peers is sorted based on priority. The client then attempts to request the file from each peer in sequence until it receives an ACK
      * Once a segment is validated, it is added to the permanent structure, allowing it to be shared with other clients. When all hashes are downloaded, the data is written to a file.
  * sendConnection: each client tracks how many times it has uploaded and shares this information when requested (every 10 hashes). Other clients use this data to select the least busy peer for downloading.
<div align="center">
  
## Tracker 

</div>

* Initialization Stage: receives a list of files that each client has at startup.
* Handles Four Main Operations:
    * Sends a list of available peers to a client requesting a specific file.
    * Decreases the number of active downloads if a client signals that it has finished downloading.
    * Marks a client as a seed once it has validated its hashes.
    * Sends all hash values of a requested file to a client.
* Once all clients have finished downloading, the tracker signals all upload and sendConnection threads to stop execution.


