# Data Governance

Having a functional pipeline is just the first half of the equation. Data Governance provides control and accountability over the data ensuring that it's a trusted source for decision-making.

Data Governance is a set of rules and mechanisms that answer the question "who" gets access to "what" data and "why". Regulations such as GDPR mandate organizations to store data securely from foreign entities. Additionally, via mechanisms like role-based access control, data is ensured to only be accesed and manipulated only by authorized identities within the organization ensuring accuracy and reliability of data.

## Data Governance in Databricks

Databricks ensures Data Governance via a set of tools:
1) Unity Catalog: The core governance tools that provides centralized access control and data lineage across all workspaces
2) Delta Lake: Ensures data reliability via ACID transactions, schema enforcement, and audit trails via transaction logs
3) Delta Sharing: Enables governed data sharing across cloud platforms
4) Cluster and Workspace Logging: Configurations for audit logging

## Unity Catalog

A unified solution for implementing Data Governance in Data Lakehouses offered by Databricks.

UC has the following 4 components:

1) Data Access Control: User-based centralized access control on files stored on the cloud via UC Objects
2) Data Audit: Enables audit logs that describe whether authorized users are using the data in the expected manner
3) Data Lineage: Ensures authenticity of data and root-cause analysis of issues by tracking datasets both upstream and downstream in a pipeline
4) Data Discoverability: A searchable data catalog to discover data within the metastore

### Account, Metastore, Workspaces, and Users

Account-scoped -> Workspace-scoped

Accounts are the top-level container; one per organization and tied to the cloud provider (AWS/Azure/GCP) that handles billing, users, and the Unity Catalog metastore(s).

A Metastore is a top-level container for data. There can only exist one per region per account. It holds the catalogs, schemas, volumes, tables, and views. Metastores are account-scoped

One metastore per region, and a workspace attaches to exactly one metastore, but a metastore can serve many workspaces in that region.

A user is an account-level identity with an email that's assigned to one or more workspaces. Users and workspaces have a many-to-many relationship and they're both account-scoped.

A workspace is a deployment of Databricks having its own URL that users can access. It isolates its own compute-layer and control-layer resources. A worksapce attaches to a Metastore making all the UC Objects in it reachable (not necessarily readable; they need access control privileges)

**Account-scoped**: UC Metastore data (Catalogs, Schemas, Volumes, Tables, Views), Users, Storage credentials, External locations.
**Workspace-scoped** : Cluster configurations, Notebooks, Git folders, Jobs, SDP Pipelines, Dashboards, and Secrets.

***Note***: Hive metastore data (two-level namespaced `schema.object`) and DBFS data (`/FileStore`) are both workspace-scoped.

## Unity Catalog Security Model

UC Catalog allows us to govern every single UC object that the Metastore holds against Principals via Role-Based Access Control and the Access Control List.

### Users, Service Principals, and Groups

These three are collectively called Principals — identities that can be granted permissions.

1) User: An account-level identity that uniquely identifies a human via their email. A user logs in and gets a personal Home folder in each workspace they're assigned to. A user is tied to a person's employment. When they leave, the account is deprovisioned and everything they had access to is forsaken.

2) Service Principal: An account-level, non-human identity used by automation — jobs, pipelines, CI/CD, external apps. It's identified by an application ID rather than an email and receives its own Unity Catalog grants and workspace permissions independently of any person.

If a production pipeline is tied to a User, it erroneously gets all permissions of that User (even ones out of the scope of the pipeline) and the pipeline becmoes contingent upon that User's account - if they leave the organization and their account is deleted, the pipeline breaks.

Hence, service principals are used to give non-human entities their own permissions decoupling them from Users entirely.

3) Group: A collection of principals — users, service principals, or other groups. Unlike the other two, groups are not something that uniquely identifies an entity. They exist purely as a collective unit of permission assignment.

Without groups, a team of 50 data engineers in the organization having 30 UC Grants would require 1500 distinct operations by the admin.
With a DE group, each User can be collectively enlisted in that group which holds the 30 privileges.

### Role-Based Access Control

A User in Unity Catalog can take one of these 4 roles:

1) Account Admin: The highest level User in UC

They have full access to every Metastore in the Databricks account. They are the only ones who can create and delete a Metastore - upon creating, they automatically become the Metastore admin and can appoint other users for the same.

2) Metastore Admin: Similar privileges to Account Admin but scoped only to the Metastore they're given the admin role for. They can also transfer ownership of the objects within that Metastore and have the capability to delete the Metastore.

3) Object Owner: Every object in the Metastore will have an owner - by default, it is the Principal that created the object but it is transferrable by the current owner or the admins. They will have full access to the object without explicit permissions being granted.

4) Object User: Majority of the Users/Service Principals - anyone in the Metastore that's neither of the admins nor the object owner. They have no access to any of the objects by default and require explicit GRANTS using Access Control Lists (ACL).

### Access Control List (ACL)

Allows access to different privileges depending on the UC Object. An Object User can be assigned one or more privileges by either one of the other three roles.

Privileges can either be granted/revoked via the UI or via SQL:

```sql
GRANT <privilege_name> ON <object_name> TO <principal_name>
REVOKE <privilege_name> ON <object_name> FROM <principal_name>
```

Chain: Metastore -> Catalog -> Schema -> {Table, View, Function}

#### Privileges grantable at each level

| Level     | Privileges                        |
|-----------|-----------------------------------|
| Metastore | CREATE CATALOG                    |
| Catalog   | USE CATALOG, CREATE SCHEMA        |
| Schema    | USE SCHEMA, CREATE TABLE/FUNCTION |
| Table     | SELECT, MODIFY                    |
| View      | SELECT                            |
| Function  | EXECUTE                           |

- SELECT & MODIFY is for tables; views are read-only (SELECT).
- Functions take EXECUTE, not SELECT.
- USE CATALOG / USE SCHEMA grant traversal only - they expose nothing alone.

#### Traversal is Mandatory

Reading one table needs the whole chain, no level skipped:
    USE CATALOG (catalog) -> USE SCHEMA (schema) -> SELECT (table)

#### Privilege Inheritance

A privilege pertaining to a child object granted on a parent object will be granted to all its child objects, including ones created in the future.
Flows DOWNWARD only.

- Granted on Catalog -> applies across all its current and future schemas/tables/views/functions:
    USE SCHEMA, CREATE TABLE/FUNCTION, SELECT, MODIFY, EXECUTE

- Granted on Schema -> applies across all its current and future tables/views/functions:
    SELECT, MODIFY, EXECUTE

#### ALL PRIVILEGES

Grantable at EVERY level - a shorthand that grants all privileges applicable at that level and every level below it.

```sql
GRANT ALL PRIVILEGES ON CATALOG <cat> TO <principal> -- full control of the catalog and everything inside it currently and in the future
```

#### ALL PRIVILEGES vs Object Owner

`ALL PRIVILEGES` is a grant. It's a shorthand that expands to every privilege applicable to that securable — SELECT, MODIFY, USE SCHEMA, EXECUTE, CREATE TABLE, etc. depending on the level. It lets you use the object fully.

Ownership is not a grant at all. It's a property stored of the object itself, like its name or its location. Every securable has exactly one owner, set to whoever created it, changeable only by `ALTER ... OWNER TO`. The owner holds ALL PRIVILEGES implicitly, and additionally can drop the object, alter it (MODIFY on Tables gives WRITE permissions but not ALTER nor DROP), and transfer ownership.

## Data-Level Security

UC Catalog enables us to achieve object-level access control, i.e, whether the Principal gets access to the entire object or not. However, this is not sufficient in many scenarios. A regional manager should only be able to see data of the employees in their own region. Analysts should be able to aggregate headcount without being able to see everyone's pay.

Data-Level security enables us to limit access to the data within an object without needing to create anew copy of the object. It does so via two mechanisms:

1) Row-Level Security (RLS): A horizontal filter. Which rows does this principal see?
2) Column-Level Masking (CLM): A vertical transform. What do the values for this column look like to this principal

### Dynamic Views

A Dynamic View allows you to query data from a table like a regular view. However, unlike a regular view, it can apply logic dynamically depending on who's querying the data. 

In a regular view, the logic is fixed and all the users see the same results. In a dynamic view, the logic is evaluated at query time based on the user by leveraging functions such as `is_account_group_member()` to determine the user's group. Hence, different users querying the same dynamic view may see different results.

To create a Group: Account -> Settings -> Identity and Access -> Groups

`is_account_group_member('group')`: Returns a Boolean based on whether the current user belongs to the group passed as the parameter

Creating a Dynamic View on top of a 'sales' table that performs Row-Level Security (done in the `WHERE` clause) and Column-Level Masking (done in the `SELECT` clause) based on the group the user belongs to.

```sql
CREATE OR REPLACE VIEW dv_sales
AS 
SELECT 
    id,
    CASE
        WHEN is_account_group_member('admin_grp') THEN email
        ELSE CONCAT(LEFT(email, 1), '***@', SPLIT_PART(email, '@', 2))
    END AS email,
    region,
    revenue
FROM sales 
WHERE is_account_group_member('admin_grp')  -- always TRUE if admin_grp; user sees all rows
    OR (is_account_group_member('uk_grp') AND region = 'uk') -- user_check AND row_check; both must be TRUE for row to qualify
    OR (is_account_group_member('us_grp') AND region = 'us')
```

Now, principals will be give access to the View instead of the Table:
`GRANT SELECT ON VIEW dv_sales TO <principal>;`

**Limitation of Dynamic Views**: A seperate Metastore object must be created and maintained that users have to query instead of the table directly.

### Row Filters and Column Masks 

Unlike dynamic views, where logic is defined in a view, the security policy is applied directly to the table using row filters and column masks. The policy is then applied automatically whenever the table is queried.

1) Row-level Filter

Enabled by attaching a function to the table that returns a BOOLEAN per row. A row is visible only if the function returns TRUE for it.

```sql
CREATE OR REPLACE FUNCTION region_filter(region_col STRING)
RETURN is_account_group_member('admin_grp') OR
(is_account_group_member('uk_grp') AND region_col = 'UK') OR
(is_account_group_member('us_grp') AND region_col = 'US');

ALTER TABLE sales
SET ROW FILTER region_filter ON (region); -- ON (argument_column(s))
```

2) Column-Level Masking

Enabled by attaching a function to the table that takes the column values and returns something of the same type.

```sql
CREATE OR REPLACE FUNCTION email_mask(email STRING)
RETURN 
CASE
    WHEN is_account_group_member('admin_grp') THEN email
    ELSE CONCAT(LEFT(email, 1), '***@', SPLIT_PART(email, '@', 2))
END;

ALTER TABLE sales -- note the different ALTER TABLE statement
ALTER COLUMN email SET MASK email_mask;
```

The Row-Level Filter and Column-Level Masking ensure that security rules are enforced automatically for all queries on the table. Even though dynamic views are more flexible as they allow operations like JOINs, these policies ensure stronger governance over the table.

**Limitation of Row Filters and Column Masks**: Not scalable. Policies need to be created and managed seperately for each table.

### Attribute-Based Access Control (ABAC)

ABAC enables centralized and scalable Data-Level security by using account-scoped tags and policies rather than directly attaching to the tables.
Hence, instead of defining the rule for every object individually, it can be applied centrally to the catalog/schema via tags and policies.

1) Governed Tags (Attributes)

An attribute is a key-value pair defined at the account-level that is applied to UC objects such as catalogs, schemas, tables, and columns. They describe charateristics of the object such as sensitivity (`pii = true`) or classification (`domain=finance`).

'Governed' tags imply that the vocabulary is centralized - three different people can't create `pii`, `PII`, and `personal_info` as three seperate tags.

**Rule**: Metastore objects inherit tags from their parent catalog and schema (overriding is allowed) except at the column level - which require explicit tags.

2) Policies (Rules)

A policy says: Within this scope, for objects carrying this tag, apply this filter/masking for these Principals

Policy Inheritance ensures policies attached at the catalog, schema, or table level automatically apply to all tables within that scope.

Two conditions must hold true at query time:
- Scope: Is the object within the scope of this policy?
- Tag: Does the object carry the tags that this policy targets?

#### Implementing ABAC

1) Step 1: Create UDFs that contain the logic for row-level filtering or column-level masking

Same UDFs as the ones created at the time of implementation of row-level filters and column-level masks policies: 
`region_filter()`: Function that filters rows based on the region column and the group that the user belongs to
`column_mask()`: Function that masks the values of the email column based on the group that the user belongs to

```sql
CREATE OR REPLACE FUNCTION region_filter(region_col STRING)
RETURN is_account_group_member('admin_grp') OR
(is_account_group_member('uk_grp') AND region_col = 'uk') OR
(is_account_group_member('us_grp') AND region_col('us'));
```

```sql
CREATE OR REPLACE FUNCTION email_mask(email STRING)
RETURN 
CASE
    WHEN is_account_group_member('admin_grp') THEN email
    ELSE CONCAT(LEFT(email, 1), '***@', SPLIT_PART(email, '@', 2))
END;
```

2) Step 2: Create Governed Tags

To create a Governed Tag via UI: Catalog -> Govern -> Governed Tags -> Create Governed Tag

`pii` = `email`: Signifies that the data contains Personally Identifiable Info and that the PII masking behaviour to be applied is for emails
`access_type` = `region`: Signifies the type of access control to be applied to the data, such as region-based or department-based

3) Step 3: Create Policies

Policies can be created at the Catalog/Schema/Table level - all child tables will inherit the policy.

To create a Policy via UI: Catalog -> Choose Catalog/Schema/Table -> Policies -> New Policy
`Principals and scope`: Define the Principals subjected to this policy and the catalog/schema/table within the scope of this policy
`Policy Type`: Row Filter or Column Mask
`Row filter function`/`Masking function`: Select function from the catalog that: evaluates each row and returns a Boolean / returns a masked value
`Function inputs`: For Row Filter Only. Map each function parameter to a specific column based on the tag it holds, such as filter via `access_type: region` tagged column
`Column conditions`: For Column Mask Only. Specify the tag the column to be masked must hold, such as mask `pii: email` tagged columns

Creating Row filter / Column masking policies via SQL:

```sql
CREATE OR REPLACE POLICY 'policy_mask_email'
ON SCHEMA catalog.schema
COMMENT opt_comment
( ROW FILTER | COLUMN MASK ) catalog.schema.function
TO `principal_1`, `principal_2`, ...
FOR TABLES 
MATCH COLUMNS hasTagValue('key', 'value') AS c1 -- match column by tag in all the tables
( USING | ON ) COLUMN c1 -- pass the aliased column as argumnts in the UDF
```

4) Step 4: Attach Tags to the Columns

Tags can be attached via Catalog Explorer UI for the Table or SQL:

```sql
ALTER TABLE customers_abac
ALTER COLUMN email SET TAGS ('pii' = 'email')
```