# 📝 Product Backlog – Population Reporting System

## Epic: Population Information Reports

---

### 🔸 User Stories (From User's Perspective)

- **US01**: As a user, I want to view all countries sorted by population so I can analyze the largest and smallest.
- **US02**: As a user, I want to get top N most populated cities per region so I can study urbanization trends.
- **US03**: As a user, I want to see capital cities sorted by population to compare national administrative centers.
- **US04**: As a user, I want a breakdown of people living in cities vs. rural areas in each continent.
- **US05**: As a user, I want to know how many people speak key languages and what % of the world they represent.

---

### 🔹 Feature List

- Country report generation
- City report generation
- Capital city report generation
- Top N reports for countries, cities, capital cities
- Population breakdown by continent, region, country
- Language speaker statistics
- Dockerized deployment
- GitHub CI/CD integration

---

### 🔧 Task List (To-Do)

- [ ] Connect to MySQL database
- [ ] Design country, city, and capital models
- [ ] Write SQL queries for country and city reports
- [ ] Implement report generators in C#
- [ ] Build top-N report logic with user input
- [ ] Generate population summary reports
- [ ] Create CLI menu interface
- [ ] Set up Dockerfile for deployment
- [ ] Configure GitHub Actions for CI
