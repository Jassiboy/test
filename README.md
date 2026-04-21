# 📘 Validation Framework Overview

## 🔍 What is a Validation Framework?
The **Validation Framework** is a structured system designed to validate all ingested files **before any processing begins**.  
Its primary goal is to ensure data quality, consistency, and reliability by performing multiple levels of validation.

---

## 🧩 Types of Validation Checks

The framework is divided into three main categories of validation:

---

### 1️⃣ File Checks
These checks validate the **integrity and properties of incoming files**.

**Includes:**
- 📄 File name validation  
- 📦 File count verification  
- ⏱️ File compression time check  
- 🏷️ File extension validation  
- 🕒 File modification timestamp check  

---

### 2️⃣ Schema Checks
These checks ensure the **structure and data types** of the data are correct.

**Includes:**
- 📐 Data type validation  
- 🧱 Schema structure validation  

---

### 3️⃣ Data Checks
These checks focus on the **actual content and quality of the data**.

**Includes:**
- 🔑 Uniqueness validation  
- 🧼 Whitespace checks  
- 🔍 Distinct value checks  
- 📊 Range validation (e.g., minimum/maximum values)  

---

## ✅ Summary
The Validation Framework ensures that:
- Files are **correct and complete**
- Schema is **consistent and expected**
- Data is **clean and reliable**

➡️ This helps prevent downstream processing errors and improves overall data quality.
