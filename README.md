# Prepare
   
   1. Download and Install vector:
https://packages.timber.io/vector/0.43.1/vector-0.43.1-x64.msi
   2. Make sure vector is in PATH

# Check

>vector --version
vector 0.43.1 (x86_64-pc-windows-msvc e30bf1f 2024-12-10 16:14:47.175528383)

# Cosmos db emulator
https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-develop-emulator?tabs=windows%2Ccsharp&pivots=api-nosql

# Open questions:
1. More sources? how to get IIS logs?
2. Clarify synchronize and guaranties question (https://vector.dev/docs/about/under-the-hood/guarantees/)
3. Retry behavior
4. Split huge message with array records into different events
