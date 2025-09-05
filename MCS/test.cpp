#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include<array>
#include <cstring>

using namespace std;

void replace(
    std::string& s,
    std::string const& toReplace,
    std::string const& replaceWith
) {
    string s3 = "";
    const int length = s.length(); 
    char* char_array = new char[length + 1]; 
    strcpy(char_array, s.c_str()); 
    for (int i = 0; i < length; i++) 
    {
        string s2(1, char_array[i]);
        std::size_t pos = s2.find(toReplace);
        if (pos != std::string::npos) {
            s2.replace(pos, toReplace.length(), replaceWith);
            s3+=s2;
        }
        else{s3+=s2;}
    }
    s=s3;
    return;
}

void Split(string s, char del,string aColumns[])
{
    string newS = "";
    string word;
    int checkpoint =0;
    int i = 0;
    if(s.substr(0,1) == " "){s.erase(0,1);}
    if(del == ' '){
        const int length = s.length(); 
        char* char_array = new char[length + 1]; 
        strcpy(char_array, s.c_str()); 
        for (int i = 0; i < length; i++) 
        {
            int count = 0;
            if(char_array[i] == ' ')
            {
                for (int j = i; j < length; j++) 
                {
                    if(char_array[j] == ' '){
                        if(checkpoint == 0 || checkpoint < j){count++;}
                        else{break;}
                    }
                    else{
                        checkpoint = j;
                        break;
                    }
                }
            }
            if(count == 1){newS += '&'; }
            else{newS += char_array[i];}
        }
    }
    else newS = s;
    stringstream ss(newS);
    while (!ss.eof()) {
        getline(ss, word, del);
        if(word != "" && word != " " && word != "\t")
        {
            replace(word, "&", " ");
            aColumns[i] = word;
            i++;
        }
    }
}

int main()
{
    string aColumns[100];
    ifstream myFile("parse.txt");
    if(!myFile.is_open()){
        cout << "Open file failed!" << endl;
    }
    string PF, BinName,tempString;
    int SWBin, HWBin, Count;
    float Percent;
 
    string myString;
    string line[100];
    int index = 0;
    while(!myFile.eof()){
        getline(myFile, line[index], '\n');
        index++;
    }
    for(int i=0;i<sizeof(line) / sizeof(*line);i++){
        if(line[i] != "")
        {
            string aData[100];
            if (line[i].find("Site") != std::string::npos) {
                Split(line[i], ' ',aColumns);
            }
            else if (line[i].find("-") != std::string::npos){
                for(int j=i+1;j<sizeof(line) / sizeof(*line);j++){
                    if(line[j] != "" && line[j].find("---") == std::string::npos)
                    {
                        Split(line[j], ' ',aData);
                        for(int k = 0;k< sizeof(aData) / sizeof(*aData);k++)
                        {
                            if(aData[k] != "" && aData[k] != " ")
                            {
                                aColumns[k] = aColumns[k]+ " ["+aData[k]+"]";
                            }
                            else{break;}
                        }
                    }
                    else{break;}
                }
                break;
            }
        }
        else{break;}
    }
    for(int i = 0;i< sizeof(aColumns) / sizeof(*aColumns);i++)
    {
        if(aColumns[i] != "")
        {
            cout << aColumns[i] << endl;
        }
        else{break;}
    }
    myFile.close();
    return 0;
}