#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>
#include "defines.h"
using namespace std;
// structura de date folosita pentru a retine datele clientilor
unordered_map<string, vector<string>> myFiles; 
// retine ce clienti stau la seed pentru fisierul respectiv
unordered_map<string, vector<int>> trackerFiles;
// hash-urile fisierelor, fara a stii care hash-uri are fiecare fisier
unordered_map<string, vector<string>> trackerHashes;
// fisierele de care au clientii nevoie
vector<string> wantedFiles;
// un numar folosit pentru a selecta mereu cel mai liber client
int uploadeTimes;
// mutex pentru uploadeTimes
pthread_mutex_t mutex;

void *download_thread_func(void *arg)
{
    int rank = *(int*) arg;
    for(int i = 0; i < (int)wantedFiles.size(); i++) {
        int findAll = 1;
        int currentSeed = 0;
        myFiles[wantedFiles[i]] = {};
        vector<string> temporaryHashes;
        int *v = NULL;
        int *v_priority = NULL;
        int nOfSeeds = 0;
        int sizeFileName = wantedFiles[i].size();
        int nOfHashes = 0;

        int askForHashes = 11;
        // primesc hash-urile de la tracker
        // in mod uzual folosite pentru securitatea protocolului
        MPI_Send(&askForHashes, 1, MPI_INT, 0, TYPE, MPI_COMM_WORLD);
        MPI_Send(&sizeFileName, 1, MPI_INT, 0, ASK_FILE_SIZE, MPI_COMM_WORLD);
        MPI_Send(wantedFiles[i].c_str(), sizeFileName + 1, MPI_CHAR, 0, ASK_FILE_NAME, MPI_COMM_WORLD);
        MPI_Recv(&nOfHashes, 1, MPI_INT, 0, ASK_N_HASHES, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char hashedString[33];
        for(int j = 0; j < nOfHashes; j++) {
            MPI_Recv(hashedString, HASH_SIZE + 1, MPI_CHAR, 0, ASK_N_HASHES, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            hashedString[32] = '\0';
            temporaryHashes.push_back(string(hashedString));
        }
        // apoi adaug in datele clientului, din datele temporare
        // pe masura ce le verific printr-un ack
        // nu le pot adauga direct in datele clientului
        // intrucat "simulam" descarcarea unui fisier real.
        while (findAll && currentSeed < nOfHashes) {
            // la 10 hash-uri descarcate, cer tracker-ului din nou lista de clienti
            // care stau la seed, in cazul in care s-am ai schimbat ceva
            if (currentSeed % 10 == 0) {
                if (v != NULL) {
                    // eliberez datele, daca erau alocate
                    delete [] v;
                    delete [] v_priority;
                    // si le asignez NULL, pentru a nu risca sa le
                    // eliberez de doua ori si sa obtin eroare double free
                    v = NULL;
                    v_priority = NULL;
                }
                // trimit tracker-ului ce fisier vreau acum
                MPI_Send(&sizeFileName, 1, MPI_INT, 0, TYPE, MPI_COMM_WORLD);
                MPI_Send(wantedFiles[i].c_str(), sizeFileName + 1, MPI_CHAR, 0, ASK_FILE_NAME_2, MPI_COMM_WORLD);
                // imi trimite inapoi ce clienti detin datele
                MPI_Recv(&nOfSeeds, 1, MPI_INT, 0, ASK_N_CLIENTS, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                v = new int[nOfSeeds + 1];
                MPI_Recv(v,nOfSeeds, MPI_INT, 0, ASK_V_CLIENTS, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // elimin rank-ul meu din vector, daca stateam la seed, ca sa nu
                // ma intreb degeaba daca am hash-ul
                for (int k = 0; k < nOfSeeds; k++) {
                    if (v[k] == rank) {
                        for (int p = k; p < nOfSeeds - 1; p++) {
                            v[p] = v[p+1];
                        }
                        nOfSeeds--;
                        break;
                    }
                }
                v_priority = new int[nOfSeeds + 1];
                // primesc vectorul de prioritati
                // ( numarul de descarcari de la fiecare client, pentru a alege mereu cel mai mic numar)
                for (int k = 0; k < nOfSeeds; k++) {
                    int priority = 1;
                    MPI_Send(&priority, 1, MPI_INT, v[k], ASK_N_PRIO, MPI_COMM_WORLD);
                    MPI_Recv(&priority, 1, MPI_INT, v[k], ASK_V_PRIO, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    v_priority[k] = priority;
                }
                // sortez vectorii
                for (int k = 0; k < nOfSeeds; k++) {
                    for (int p = k + 1; p < nOfSeeds - 1; p++) {
                        if (v_priority[k] > v_priority[p]) {
                            swap(v_priority[k], v_priority[p]);
                            swap(v[k], v[p]);
                        }
                    }
                }
            }
            // daca am validat macar un hash, atunci pot sa ofer spre descarcare
            // acel hash si altor clienti
            if (currentSeed == 1) {
                int operation = 7;
                MPI_Send(&operation, 1, MPI_INT, 0, TYPE, MPI_COMM_WORLD);
                MPI_Send(&sizeFileName, 1, MPI_INT, 0, ASK_FILE_SIZE_3, MPI_COMM_WORLD);
                MPI_Send(wantedFiles[i].c_str(), sizeFileName + 1, MPI_CHAR, 0, ASK_FILE_NAME_3, MPI_COMM_WORLD);
            }
            int bestValue = 0;
            // intreb clientii la rand in functie de care e cel mai putin ocupat, daca are cineva
            // hash-ul pe care il caut, cand il gasesc ma opresc
            // daca nu il are nimeni inseamna ca am terminat de descarcat fisierul
            while (bestValue < nOfSeeds && currentSeed < (int)temporaryHashes.size()) {
                int bestChoice = v[bestValue];
                MPI_Send(&sizeFileName, 1, MPI_INT, bestChoice, ASK_FILE_SIZE_4, MPI_COMM_WORLD);
                MPI_Send(wantedFiles[i].c_str(), sizeFileName + 1, MPI_CHAR, bestChoice, ASK_FILE_NAME_4, MPI_COMM_WORLD);
                MPI_Send(&currentSeed, 1, MPI_INT, bestChoice, ASK_SEED, MPI_COMM_WORLD);
                int check = 0;
                MPI_Recv(&check, 1, MPI_INT, bestChoice, ASK_ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if (check == -1) {
                    bestValue++;
                } else {
                    // am gasit datele cautate
                    myFiles[wantedFiles[i]].push_back(temporaryHashes[currentSeed]);
                    break;
                }
            }
            currentSeed++;
            if (bestValue >= nOfSeeds || currentSeed >= (int)temporaryHashes.size()) {
                // eliberez datele
                // inainte de a da iesi din loop
                if (v != NULL) {
                    delete [] v;
                    v = NULL;
                }
                if (v_priority != NULL) {
                    delete[] v_priority;
                    v_priority = NULL;
                }
                findAll = 0;
                break;
            }
        }

    }
    int finished = -1;
    // trimit tracker-ului ca am terminat de descarcat toate fisierele
    // pentru a opri thread-ul de download DAR PASTRA cel de upload
    MPI_Send(&finished, 1, MPI_INT, 0, TYPE, MPI_COMM_WORLD);
    // scriu datele in fiserele respective
    for (int i = 0; i < (int)wantedFiles.size(); i++) {
        char value = wantedFiles[i][4];
        string value_str;
        value_str += value;
        string outFile = "client" + to_string(rank) + "_file" + value_str;
        ofstream fout(outFile);
        for (int j = 0; j < (int)myFiles[wantedFiles[i]].size(); j++) {
            if (j < (int)myFiles[wantedFiles[i]].size() - 1)
                fout << myFiles[wantedFiles[i]][j] << endl;
            else
                fout << myFiles[wantedFiles[i]][j];
        }
    }

    return NULL;
}

void *sendNumberConnections(void *arg) {
    while (1) {
        int type;
        MPI_Status status;
        // astept pana primesc o cerere
        MPI_Recv(&type, 1, MPI_INT, MPI_ANY_SOURCE, ASK_N_PRIO, MPI_COMM_WORLD, &status);
        if (type == -1) {
            // daca toti clienti au terminat de descarcat ma opresc
            break;
        } else {
            // altfel, cu ajutorul unui mutex ( just to be safe) trimit numarul de upload-uri
            pthread_mutex_lock(&mutex);
            MPI_Send(&uploadeTimes, 1, MPI_INT, status.MPI_SOURCE, ASK_V_PRIO, MPI_COMM_WORLD);
            pthread_mutex_unlock(&mutex);
        }
    }
    return NULL;
}

void *upload_thread_func(void *arg)
{
    int type = 0;
    MPI_Status status;
    while (1) {
        MPI_Recv(&type, 1, MPI_INT, MPI_ANY_SOURCE, ASK_FILE_SIZE_4, MPI_COMM_WORLD, &status);
        if (type > 0) {
            char *data = new char[type + 1];
            MPI_Recv(data,type + 1, MPI_CHAR, status.MPI_SOURCE, ASK_FILE_NAME_4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            data[type] = '\0';
            string recvName(data);
            delete[] data;
            int currentSeed;
            MPI_Recv(&currentSeed, 1, MPI_INT, status.MPI_SOURCE, ASK_SEED, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // verific daca detin access la seed-ul cautat
            // daca da, ii trimit un ack, daca nu ii trimit un nack
            if (currentSeed < (int)myFiles[recvName].size()) {
                pthread_mutex_lock(&mutex);
                uploadeTimes++;
                pthread_mutex_unlock(&mutex);
                int ack = 1;
                MPI_Send(&ack, 1, MPI_INT, status.MPI_SOURCE, ASK_ACK, MPI_COMM_WORLD);
            } else {
                int nack = -1;
                MPI_Send(&nack, 1, MPI_INT, status.MPI_SOURCE, ASK_ACK, MPI_COMM_WORLD);
            }
        } else {
            // verifici daca trebuie sa inchid thread-ul
            if(type == -1) break;
        }
    }

    return NULL;
}
void initTracker(int numtasks) {
    for (int i = 0; i < numtasks - 1; i++) {
        int len;
        MPI_Status status;
        MPI_Recv(&len, 1, MPI_INT, MPI_ANY_SOURCE, ASK_N_OF_FILES, MPI_COMM_WORLD, &status);
        int src = status.MPI_SOURCE;
        for (int j = 0; j < len; j++) {
            int lenString;
            MPI_Recv(&lenString, 1, MPI_INT, status.MPI_SOURCE, ASK_SIZE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            char *data = new char[lenString + 1];
            MPI_Recv(data, lenString + 1, MPI_CHAR, status.MPI_SOURCE, ASK_HASH_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            int sizeElements;
            data[lenString] = '\0';
            string strData(data);
            delete[] data;
            int check;
            if (trackerFiles.find(strData) == trackerFiles.end()) {
                check = 1;
                trackerFiles[strData] = {src};
                MPI_Send(&check, 1, MPI_INT, status.MPI_SOURCE, ASK_IF_WE_HAVE_THE_DATA, MPI_COMM_WORLD);
                MPI_Recv(&sizeElements, 1, MPI_INT, status.MPI_SOURCE, ASK_N_HASHES_2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                char *hashData = new char[HASH_SIZE + 1];
                trackerHashes[strData] = {};
                for (int k = 0; k < sizeElements; k++) {
                    MPI_Recv(hashData, HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, ASK_HASHES_2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    hashData[32] = '\0';
                    string hashString(hashData);
                    trackerHashes[strData].push_back(hashString);
                }
                delete [] hashData;
            } else {
                check = 0;
                trackerFiles[strData].push_back(src);
                MPI_Send(&check, 1, MPI_INT, status.MPI_SOURCE, ASK_IF_WE_HAVE_THE_DATA, MPI_COMM_WORLD);
            }
        }
    }
    for (int i = 1; i < numtasks; i++) {
        int ack = i;
        // o data ce am terminat de populat datele, las clienti sa inceapa download-ul
        MPI_Send(&ack, 1, MPI_INT, i, ASK_N_OF_FILES, MPI_COMM_WORLD);
    }

}

void updateClientVector(MPI_Status status) {
    int sizeString = 0;
    MPI_Recv(&sizeString, 1, MPI_INT, status.MPI_SOURCE, ASK_FILE_SIZE_3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    char *fileRequested = new char[sizeString + 1];
    MPI_Recv(fileRequested, sizeString + 1, MPI_CHAR, status.MPI_SOURCE, ASK_FILE_NAME_3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    fileRequested[sizeString] = '\0';
    string fileName(fileRequested);
    delete [] fileRequested;
    trackerFiles[fileName].push_back(status.MPI_SOURCE);
}

void sendHashDataToClient(MPI_Status status) {
    int sizeString = 0;
    MPI_Recv(&sizeString, 1, MPI_INT, status.MPI_SOURCE, ASK_FILE_SIZE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    char *fileRequested = new char[sizeString + 1];
    MPI_Recv(fileRequested, sizeString + 1, MPI_CHAR, status.MPI_SOURCE, ASK_FILE_NAME, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    string fileStringReq(fileRequested);
    int nOfHashes = trackerHashes[fileStringReq].size();
    MPI_Send(&nOfHashes, 1, MPI_INT, status.MPI_SOURCE, ASK_N_HASHES, MPI_COMM_WORLD);
    for (int k = 0; k < nOfHashes; k++) {
        MPI_Send(trackerHashes[fileStringReq][k].c_str(), HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, ASK_N_HASHES, MPI_COMM_WORLD);
    }
    delete [] fileRequested;
}

void sendClientVectorToClient(MPI_Status status, int operation) {
    char *fileRequested = new char[operation + 1];
    MPI_Recv(fileRequested, operation + 1, MPI_CHAR, status.MPI_SOURCE, ASK_FILE_NAME_2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    fileRequested[operation] = '\0';
    string fileName(fileRequested);
    delete []fileRequested;
    int sizeOfRequested = trackerFiles[fileName].size();
    MPI_Send(&sizeOfRequested, 1, MPI_INT, status.MPI_SOURCE, ASK_N_CLIENTS, MPI_COMM_WORLD);
    MPI_Send(trackerFiles[fileName].data(), sizeOfRequested, MPI_INT, status.MPI_SOURCE, ASK_V_CLIENTS, MPI_COMM_WORLD);
}

void tracker(int numtasks, int rank) {
    // initializarea datelor
    // ( cer tuturor clientilor sa imi dea datele pe care le au pentru a imi popula structurile de date)
    initTracker(numtasks);
    int taskToBeSolved = numtasks - 1;
    while (taskToBeSolved) {
        int operation = 0;
        MPI_Status status;
        MPI_Recv(&operation, 1, MPI_INT, MPI_ANY_SOURCE, TYPE, MPI_COMM_WORLD, &status);
        if (operation > 0 && operation != 7 && operation != 11) {
            // trebuie trimis vectorul care contine clientii care stau la seed
            // pentru fisierul cerut
            sendClientVectorToClient(status, operation);
        } else if (operation < 0) {
            // un client a terminat de downloadat
            taskToBeSolved--;
        } else if (operation == 7) {
            // actualizez lista de clienti pentru un fiser
            updateClientVector(status);
        } else if (operation == 11) {
            // folosit pentru a cere hash-urile de clienti
            // ( in mod normal folosite pentru securitate)
            sendHashDataToClient(status);
        }
    }

    for(int i = 1; i < numtasks; i++) {
        int finishedProcessing = -1;
        MPI_Send(&finishedProcessing, 1, MPI_INT, i, ASK_FILE_SIZE_4, MPI_COMM_WORLD);
        MPI_Send(&finishedProcessing, 1, MPI_INT, i, ASK_N_PRIO, MPI_COMM_WORLD);
    }
    
}

void initPeer(int rank) {
    string fileName = "in" + to_string(rank) + ".txt";
    ifstream fin(fileName);
    int nOfReadings;
    fin >> nOfReadings;
    for (int i = 0; i < nOfReadings; i++) {
        string inputF;
        int nOfChunks;

        fin >> inputF;
        fin >> nOfChunks;
        myFiles[inputF] = {};
        for (int j = 0; j < nOfChunks; j++) {
            string data;
            fin >> data;
            myFiles[inputF].push_back(data);
        }
    }
    int needData;
    fin >> needData;
    for (int i = 0; i < needData; i++) {
        string needFile;
        fin >> needFile;
        wantedFiles.push_back(needFile);
    }
    int sizeD = myFiles.size();
    // trimit datele citite la tracker
    MPI_Send(&sizeD, 1, MPI_INT, 0, ASK_N_OF_FILES, MPI_COMM_WORLD);
    for (auto& elem : myFiles) {
        int dataLen = elem.first.size();
        MPI_Send(&dataLen, 1, MPI_INT, 0, ASK_SIZE_DATA, MPI_COMM_WORLD);
        MPI_Send(elem.first.c_str(), dataLen + 1, MPI_CHAR, 0, ASK_HASH_DATA, MPI_COMM_WORLD);
        int sizeElements = elem.second.size();
        int check = 0;
        MPI_Recv(&check, 1, MPI_INT, 0, 30, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (check == 1) {
            MPI_Send(&sizeElements, 1, MPI_INT, 0, ASK_N_HASHES_2, MPI_COMM_WORLD);
            for (int i = 0; i < sizeElements; i++) {
                MPI_Send(elem.second[i].c_str(), HASH_SIZE + 1, MPI_CHAR, 0, ASK_HASHES_2, MPI_COMM_WORLD);
            }
        }
    }
    int ack;
    MPI_Recv(&ack, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

void peer(int numtasks, int rank) {
    pthread_mutex_init(&mutex, NULL);
    pthread_t download_thread;
    pthread_t upload_thread;
    pthread_t sendConnections;
    void *status;
    int r;
    // initializarea datelor ( citirea lor din fisiere)
    initPeer(rank);
    r = pthread_create(&sendConnections, NULL, sendNumberConnections, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(sendConnections, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
    pthread_mutex_destroy(&mutex);
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
