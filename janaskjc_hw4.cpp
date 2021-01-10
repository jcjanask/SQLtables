// Copyright Joe Janaskie 2020
#define MYSQLPP_MYSQL_HEADERS_BURIED
#include <mysql++/mysql++.h>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

void interactive() {
    // Connect to database with: database, server, userID, password
    mysqlpp::Connection myDB("cse278", "localhost", "cse278", "S3rul3z");
    // Variable to build query string
    std::string qString;
    // Get user input for query
    while (getline(std::cin, qString) && qString != "Quit") {
        // Create a query
        mysqlpp::Query query = myDB.query();
      query << qString;
        try {
            // Check query is correct
            query.parse();
            // Execute query
            mysqlpp::StoreQueryResult result = query.store();
            // Results is a 2D vector of mysqlpp::String objects.
            // Print the results.
            std::cout << "-----Query Result-----" << std::endl;
            for (size_t row = 0; (row < result.size()); row++) {
  std::cout << "| ";
                 for (size_t col = 0; (col < result[row].size()); col++) {
                 std::cout << result[row][col] << " | ";
                 }
                 std::cout << "\n";
            }
            std::cout << "------End Result------" << std::endl;
        } catch(mysqlpp::BadQuery e) {
            std::cerr << "Query: " << qString <<std::endl;
            std::cerr << "Query is not correct SQL syntax" <<std::endl;
        }
    }
}

std::string generateLoadQuery(std::string& line) {
    // Create base insert query string
std::string str = "INSERT INTO ";
    // Split file on commas
std::vector<std::string> result;
boost::split(result, line, boost::is_any_of(","));
    // Start building query from split files (table name)
str += result[0] + "_tmp ";
    // Strings to hold attributes and values
    // Build attribute and value strings
std::string attributes = "(";
std::string values = "(";
for (int i = 1; i < result.size(); i++) {
attributes += result[i].substr(0, result[i].find(":"));
values += result[i].substr(result[i].find(":") + 1);
if (i != result.size() - 1) {
attributes += ", ";
values += ", ";
}
}
attributes += ")";
values += ")";
    // Form full query string
str += attributes + " VALUES " + values + ";";
return str;
}


void loadData(std::string& path) {
    // Open file stream
    std::ifstream input(path);
    // Connect to database with: database, server, userID, password
    mysqlpp::Connection myDB("cse278", "localhost", "cse278", "S3rul3z");
    // Some necessary variables for the file IO
    std::string str;
    // Read file line-by-line
    int i = 1;
    while (std::getline(input, str)) {
        // Create query string from current line
        std::string ret = generateLoadQuery(str);
        // Create mysql++ query
        mysqlpp::Query query = myDB.query();
        query << ret;
        try {
            // Check query is correct
            query.parse();
            // Execute Query
            mysqlpp::StoreQueryResult result = query.store();
            std::cout << "Data Line " << i << " Loaded" << std::endl;
            i++;
        } catch(mysqlpp::BadQuery e) {
            std::cerr << "Query: " << ret <<std::endl;
            std::cerr << "Query is not correct SQL syntax" <<std::endl;
       }
    }
}


std::string generateCreateQuery(std::string& line) {
std::string str = "CREATE TABLE ";
std::vector<std::string> result;
boost::split(result, line, boost::is_any_of(","));
    // Start building query from split files (table name)
str += result[0].substr(result[0].find(":") + 1) + " (";
    // Strings to hold attributes and values
    // Build attribute and value strings
std::vector <std::string> words;
std::string key = "";
std::string t = "";
for (int i = 1; i < result.size(); i++) {
boost::split(words, result[i], boost::is_any_of(":"));
str += words[0] + " ";
for (int j = 1; j < words.size(); j++) {
if (words[j] == "key") {
key = words[0];
str += ", ";
continue;
}
if (j != 0) {
t = words[j];
boost::to_upper(t);
}
if (t == "NOT_NULL") {
t.replace(3, 1, " ");
}
str += t + " ";
if (j == words.size() - 1 && i != result.size() - 1)
str += ",";
}
}
if (key != "") str += ", PRIMARY KEY (" + key + ") ";
str += " );";
return str;
}
void createTable(std::string& path) {
std::ifstream in(path);
mysqlpp::Connection myDB("cse278", "localhost", "cse278", "S3rul3z");
    // Some necessary variables for the file IO
    std::string str;
    // Read file line-by-line
    int i = 1;
    while (std::getline(in, str)) {
    std::string fin = generateCreateQuery(str);
    mysqlpp::Query query = myDB.query();
    query << fin;
      try {
       query.parse();
       mysqlpp::StoreQueryResult result = query.store();
       std::cout << "Table janaskjc_myTable" << i << " Created" << std::endl;
       i++;
    } catch (mysqlpp::BadQuery e) {
            std::cerr << "Query: " << fin <<std::endl;
            std::cerr << "Query is not correct SQL syntax" <<std::endl;
            i++;
     }
}
}

void dropTable(std::string& path) {
    mysqlpp::Connection myDB("cse278", "localhost", "cse278", "S3rul3z");
    std::string str = "DROP TABLE " + path + ";";
    mysqlpp::Query query = myDB.query();
    query << str;
      try {
       query.parse();
       mysqlpp::StoreQueryResult result = query.store();
       std::cout << "Table janaskjc_" << path << " Dropped." << std::endl;
    } catch (mysqlpp::BadQuery e) {
            std::cerr << "Query: " << str <<std::endl;
            std::cerr << "Query is not correct SQL syntax" <<std::endl;
     }
}

void writeTable(std::string in) {
std::ifstream input(in);
std::ofstream out("write_table.txt");
mysqlpp::Connection myDB("cse278", "localhost", "cse278", "S3rul3z");
std::string line;
while (getline(input, line)) {
mysqlpp::Query query = myDB.query();
query << line;

try {
query.parse();
mysqlpp::StoreQueryResult result = query.store();
out << "-----Query Result-----" << std::endl;
for (size_t row = 0; (row < result.size()); row++) {
out << "| ";
for (size_t col = 0; (col < result[row].size()); col++) {
                 out << result[row][col] << " | ";
                 }
                 out << "\n";
            }
            out << "------End Result------" << std::endl;
        } catch(mysqlpp::BadQuery e) {
            std::cerr << "Query: " << line <<std::endl;
            std::cerr << "Query is not correct SQL syntax" <<std::endl;
        }
}
out.close();
}

int main(int argc, char *argv[]) {
    // Ensure files are specified
    if (argc < 2 || argc > 3) {
        std::cerr << "Invalid number of arguments" << std::endl;
       return 1;
    }
    std::string option = argv[1];

    if (option == "-I") {
        /* do something */
        interactive();
    } else if (option == "-L" && argc == 3) {
        /* do something else */
        std::string fileName(argv[2]);
        loadData(fileName);
    } else if (option == "-C" && argc == 3) {
        std::string path(argv[2]);
        createTable(path);
    } else if (option == "-D" && argc == 3) {
     std::string path(argv[2]); 
     dropTable(path);
    } else if (option == "-W") {
      std::string input(argv[2]);
      writeTable(input);
} else {
        std::cerr << "Invalid input" << std::endl;
        return 1;
    }

    // All done
    return 0; 
}


