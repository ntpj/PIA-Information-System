# Portfolio Investment Abroad (PIA) Information System

This project is a part of my internship work at The Bank of Thailand back in Janurary 2024 to May 2024. The scope of this project, that is available on this repository, is to create an information system that reduce automation process as much as possible, and integrate methods of data acquisition to compare with / improve Portfolio Investment Abroad (PIA) data quality.

The experimented methods in this project covers the following techniques, note that not all of the techniques are actually implemented due to resource constraints:
1. Web Scrape (cbonds, markets.businessinsider, sec.gov using Selenium and BeautifulSoup4)
2. LLM (via GPT4ALL Python Library, ChatGPT API, Automated Copilot using Selenium)
3. OCR (via EasyOCR Python Library)
4. API (via Investpy Python Library)

The optimization in the system is done by using Set Search and Binary Search when applicable, and only use Linear Search when either method is inconvenient. This allows for a total of faster processing time, reducing the total processing time (manual process included) from ~2 days down to ~2 hours. In summary, this new system only processes only some parts of the whole Foreign Sector Statistics data (e.g., exclusion of BPIA) which are included in the old system, but the total processing time should still be faster than the previous system (theoretically) 

*Disclaimer: All codes posted to this repository is currently the intellectual property of The Bank of Thailand, and I was granted the permission as the developer to public this project.
