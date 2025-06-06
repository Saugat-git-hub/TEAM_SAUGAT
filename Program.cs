using System;
using System.Data;
using MySql.Data.MySqlClient;

namespace PopulationReportingSystem
{
    public class DatabaseHelper
    {
        private readonly string _connectionString;

        public DatabaseHelper(string connectionString)
        {
            _connectionString = connectionString;
        }

        // Execute query and return DataTable
        public DataTable ExecuteQuery(string query, params MySqlParameter[] parameters)
        {
            var dataTable = new DataTable();

            try
            {
                using (var connection = new MySqlConnection(_connectionString))
                using (var command = new MySqlCommand(query, connection))
                {
                    connection.Open();
                    if (parameters != null && parameters.Length > 0)
                        command.Parameters.AddRange(parameters);

                    using (var reader = command.ExecuteReader())
                    {
                        dataTable.Load(reader);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Database error: {ex.Message}");
                throw;
            }

            return dataTable;
        }

        // Execute non-query commands
        public int ExecuteNonQuery(string commandText, params MySqlParameter[] parameters)
        {
            try
            {
                using (var connection = new MySqlConnection(_connectionString))
                using (var command = new MySqlCommand(commandText, connection))
                {
                    connection.Open();
                    if (parameters != null && parameters.Length > 0)
                        command.Parameters.AddRange(parameters);
                    return command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Database error: {ex.Message}");
                throw;
            }
        }

        // Execute scalar operations
        public object ExecuteScalar(string commandText, params MySqlParameter[] parameters)
        {
            try
            {
                using (var connection = new MySqlConnection(_connectionString))
                using (var command = new MySqlCommand(commandText, connection))
                {
                    connection.Open();
                    if (parameters != null && parameters.Length > 0)
                        command.Parameters.AddRange(parameters);
                    return command.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Database error: {ex.Message}");
                throw;
            }
        }
    }

    public class PopulationReporter
    {
        private readonly DatabaseHelper _dbHelper;

        public PopulationReporter(string connectionString)
        {
            _dbHelper = new DatabaseHelper(connectionString);
        }

        // Country Reports
        public DataTable GetAllCountriesByPopulation()
        {
            string query = @"
                SELECT 
                    code AS Code,
                    name AS Name,
                    continent AS Continent,
                    region AS Region,
                    population AS Population,
                    capital AS Capital
                FROM 
                    countries
                ORDER BY 
                    population DESC";

            return _dbHelper.ExecuteQuery(query);
        }

        public DataTable GetTopNPopulatedCountries(int topN)
        {
            string query = @"
                SELECT 
                    code AS Code,
                    name AS Name,
                    continent AS Continent,
                    region AS Region,
                    population AS Population,
                    capital AS Capital
                FROM 
                    countries
                ORDER BY 
                    population DESC
                LIMIT @topN";

            var parameter = new MySqlParameter("@topN", MySqlDbType.Int32)
            {
                Value = topN
            };

            return _dbHelper.ExecuteQuery(query, parameter);
        }

        // City Reports
        public DataTable GetAllCitiesByPopulation()
        {
            string query = @"
                SELECT 
                    c.name AS Name,
                    co.name AS Country,
                    c.district AS District,
                    c.population AS Population
                FROM 
                    cities c
                JOIN 
                    countries co ON c.country_code = co.code
                ORDER BY 
                    c.population DESC";

            return _dbHelper.ExecuteQuery(query);
        }

        // Capital City Reports
        public DataTable GetAllCapitalCitiesByPopulation()
        {
            string query = @"
                SELECT 
                    ci.name AS Name,
                    co.name AS Country,
                    ci.population AS Population
                FROM 
                    cities ci
                JOIN 
                    countries co ON ci.id = co.capital
                ORDER BY 
                    ci.population DESC";

            return _dbHelper.ExecuteQuery(query);
        }

        // Population Breakdown Reports
        public DataTable GetContinentPopulationBreakdown()
        {
            string query = @"
                SELECT 
                    continent AS Name,
                    SUM(population) AS TotalPopulation,
                    ROUND(SUM(city_population) / SUM(population) * 100, 2) AS PercentageInCities,
                    ROUND((SUM(population) - SUM(city_population)) / SUM(population) * 100, 2) AS PercentageNotInCities
                FROM (
                    SELECT 
                        c.continent,
                        c.population,
                        (SELECT SUM(ci.population) 
                         FROM cities ci 
                         WHERE ci.country_code = c.code) AS city_population
                    FROM 
                        countries c
                ) AS subquery
                GROUP BY 
                    continent";

            return _dbHelper.ExecuteQuery(query);
        }

        // Language Reports
        public DataTable GetLanguageStatistics()
        {
            string query = @"
                SELECT 
                    cl.language AS Language,
                    SUM(cl.percentage * co.population / 100) AS Speakers,
                    ROUND(SUM(cl.percentage * co.population / 100) / 
                          (SELECT SUM(population) FROM countries) * 100, 2) AS WorldPercentage
                FROM 
                    country_languages cl
                JOIN 
                    countries co ON cl.country_code = co.code
                WHERE 
                    cl.language IN ('Chinese', 'English', 'Hindi', 'Spanish', 'Arabic')
                GROUP BY 
                    cl.language
                ORDER BY 
                    Speakers DESC";

            return _dbHelper.ExecuteQuery(query);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            // Make sure your PuTTY SSH tunnel is running: local port 3307 -> remote MySQL server port 3306
            // Update the connection string below with your actual database name, user, and password
            string connectionString = "Server=scebe-db-liv-01.napier.ac.uk;Database=40742463;User=40742463;Password=GkcRaNGB;";

            var reporter = new PopulationReporter(connectionString);

            try
            {
                Console.WriteLine("=== Top 10 Most Populated Countries ===");
                var topCountries = reporter.GetTopNPopulatedCountries(10);
                PrintDataTable(topCountries);

                Console.WriteLine("\n=== Continent Population Breakdown ===");
                var continentBreakdown = reporter.GetContinentPopulationBreakdown();
                PrintDataTable(continentBreakdown);

                Console.WriteLine("\n=== Language Statistics ===");
                var languageStats = reporter.GetLanguageStatistics();
                PrintDataTable(languageStats);
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred: " + ex.Message);
            }
        }

        static void PrintDataTable(DataTable table)
        {
            // Print column headers
            foreach (DataColumn column in table.Columns)
            {
                Console.Write($"{column.ColumnName,-25}");
            }
            Console.WriteLine("\n" + new string('-', table.Columns.Count * 25));

            // Print rows
            foreach (DataRow row in table.Rows)
            {
                foreach (var item in row.ItemArray)
                {
                    Console.Write($"{item,-25}");
                }
                Console.WriteLine();
            }
        }
    }
}