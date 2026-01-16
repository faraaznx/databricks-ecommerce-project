## Bronze
- `Orders` json files are to be incrementally loaded and schema is being enforced. Any deviation from the schema will cause the pipeline to fail at the bronze processing
- `Products` Excel files are incrementally loaded, meaning the source containers get new files every day. Schema is nt forced. Any changes in the schema will be captured in the `rescue` column.
- `Customer` csv is assumed to be a full file. Every time a new file comes, it is going to replace the older file. All the customer will always be present in a single file.

## Silver
- `Orders` table has a schema enforced. Deviation will cause failure
- `Products` table as schema enforced for columns required in the final agg table. Deviation will cause failure.
- `Customers` table has schema enforced for columns required in the final agg table. Deviation will cause failure. 

## Cases to Handle
- If there's no file or empty file.
- Corrupt rows
- When a schema is changed for the joining table (joining key is missing)
