This C# class was part of a large application that is not available in this demo. The class is merely to show my ability to code complex classes and methods in C#.

The goal of the validation class was to read in data from SAP using the REAT_TABLE RFC to validate G/L journal entries.
Given that the READ_TABLE RFC can only return one table result at a time, it was necessary to come up with an optimized way to join tables in memory.
This class makes heavy use of dictionaries to accomplish this requirement.
