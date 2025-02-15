# Dimensional Directory System Overview

## Introduction

The **Dimensional Directory** system is a robust framework designed to
manage complex data structures through modularity and scalability.
Central to this system are **L and S identifiers** ([dbidL]{.title-ref}
and [dbidS]{.title-ref}), the **\`.LStable\`** file, and various
configuration files ([.dddoc]{.title-ref}, [.ddapc]{.title-ref}, and
[.ddopm]{.title-ref}) that facilitate the creation of abstract objects
and control planes known as **AddressPlanes**.

This document provides a comprehensive guide to these components,
detailing their purpose, structure, implementation, and best practices
to ensure a consistent and efficient approach to data management within
the Dimensional Directory system.

## L and S Identifiers

The Dimensional Directory framework employs **L and S identifiers**
([dbidL]{.title-ref} and [dbidS]{.title-ref}) to maintain clarity,
modularity, and scalability in dynamic and hierarchical data management
systems. These identifiers ensure that complex systems remain organized
and accessible.

\### 1. Purpose of L and S Identifiers

-   **Long Identifier (L):**
    -   A human-readable, descriptive name for databases or entities.
    -   Facilitates clarity and context in understanding and managing
        resources.
    -   *Example:* [UserDatabase2024]{.title-ref},
        [TransactionLogs]{.title-ref}.
-   **Short Identifier (S):**
    -   A compact representation, often numeric or coded, for efficiency
        and brevity.
    -   Optimized for scenarios with storage or length constraints.
    -   *Example:* [001]{.title-ref}, [TX2024]{.title-ref}.

\### 2. Interplay Between L and S Identifiers

The dual identifier system combines the strengths of both formats:

1.  **Uniqueness:** Ensures distinct identification even in distributed
    systems.
2.  **Dynamic Reference:** [dbidL]{.title-ref} offers context, while
    [dbidS]{.title-ref} provides quick lookup.
3.  **Scalability:** Supports modular and hierarchical structures for
    seamless growth.

\### 3. Dynamic Naming System

Dynamic naming leverages templates and placeholders to adapt file and
directory paths based on runtime configurations of [dbidL]{.title-ref}
and [dbidS]{.title-ref}.

**Example of Template-Based Naming:**

-   **Template:** [dd_data/{dbidL}/AB-{dbidS}.json]{.title-ref}
-   **Resolved Path:**
    -   Given [dbidL = \"UserData\"]{.title-ref} and [dbidS =
        \"001\"]{.title-ref}, the result is:
        -   [dd_data/UserData/AB-001.json]{.title-ref}

This system ensures:

-   **Consistency:** Uniform patterns for files and directories.
-   **Separation of Concerns:** Logical and physical isolation of
    entities.
-   **Flexibility:** Easy adaptation to new structures and requirements.

\### 4. Maintaining Separation

\#### Directory Structure

Each [dbidL]{.title-ref} maps to a root directory. Files within this
directory are indexed by [dbidS]{.title-ref}.

**Example:**

``` none
dd_data/
├─ UserData/
│   ├─ AB-001.json
│   ├─ DD-001.json
│   └─ .LStable
└─ TransactionLogs/
    ├─ AB-002.json
    ├─ DD-002.json
    └─ .LStable
```

\#### Dynamic Placeholder Replacement

Placeholders such as [{dbidL}]{.title-ref} and [{dbidS}]{.title-ref}
dynamically resolve based on runtime configurations, creating or
accessing paths as needed.

\#### Modular Wrappers

Wrappers like [JSONDatabaseWrapper]{.title-ref} enforce the use of
[dbidL]{.title-ref} and [dbidS]{.title-ref} during initialization,
ensuring:

1.  **Compliance with Naming Conventions**
2.  **Logical Isolation to Prevent Data Cross-Contamination**

\### 5. Implementation in Code

Here's an example demonstrating the integration of L and S identifiers
in the Dimensional Directory framework:

``` python
from core.manager.dd_manager import DDManager

# Initialize the manager
manager = DDManager(base_path="dimensional_directory")

# Create a database wrapper
json_db = manager.create_wrapper(
    wrapper_class="JSONDatabaseWrapper",
    dbidL="UserData",
    dbidS="001"
)

# Perform operations
json_db.add_data("user1", {"name": "Alice", "role": "admin"})
print(json_db.get_data("user1"))
```

\### 6. Benefits of the L and S System

1.  **Clarity:** Descriptive [dbidL]{.title-ref} aids understanding and
    debugging.
2.  **Compactness:** Efficient [dbidS]{.title-ref} suits constrained
    environments.
3.  **Scalability:** Supports hierarchical and modular structures.
4.  **Separation:** Ensures distinct logical and physical organization.

\### 7. Best Practices

1.  **Define Standards:** Establish naming conventions for
    [dbidL]{.title-ref} and [dbidS]{.title-ref}.
2.  **Avoid Collisions:** Ensure [dbidS]{.title-ref} is unique within
    its [dbidL]{.title-ref} namespace.
3.  **Use Automation:** Employ tools for consistent identifier
    generation and validation.
4.  **Periodic Audits:** Regularly review identifier usage to maintain
    integrity.

## The [.LStable]{.title-ref} File

The **\`.LStable\`** file is a critical component for maintaining
mappings between [dbidL]{.title-ref} (Long Identifiers) and
[dbidS]{.title-ref} (Short Identifiers) across all [dd_data]{.title-ref}
directories in the Dimensional Directory system. By standardizing its
usage and structure, we ensure a consistent and efficient approach to
managing identifier relationships, simplifying both development and
maintenance.

\### 1. Purpose

-   **Registry:** Acts as a single source of truth for identifier
    relationships.
-   **Consistency:** Uniform structure across all directories.
-   **Dynamic Reference:** Enables automated discovery and validation.

\### 2. Structure of the [.LStable]{.title-ref} File

The [.LStable]{.title-ref} file is a plain text file where each line
represents a mapping in the format:

{dbidL=dbidS}

**Example File:**

``` plaintext
UserData=001
TransactionLogs=002
AnalyticsData=003
```

\### 3. Usage Guidelines

\#### Placement

-   The [.LStable]{.title-ref} file must exist in the root of each
    [dd_data/{dbidL}]{.title-ref} directory.

**Example Directory Structure:**

``` none
dd_data/
├─ UserData/
│   ├─ AB-001.json
│   ├─ DD-001.json
│   └─ .LStable
├─ TransactionLogs/
│   ├─ AB-002.json
│   ├─ DD-002.json
│   └─ .LStable
└─ AnalyticsData/
    ├─ AB-003.json
    ├─ DD-003.json
    └─ .LStable
```

\#### Content Management

-   Each [dbidL]{.title-ref} should have a unique mapping to a
    [dbidS]{.title-ref}.
-   Duplicate or conflicting entries are disallowed.
-   Lines must follow the [{dbidL=dbidS}]{.title-ref} format strictly.

\### 4. Implementation

\#### Creation and Update Script

To automate the creation and management of [.LStable]{.title-ref} files,
use the following utility script:

``` python
import os

def create_or_update_lstable(directory, mappings):
    """
    Create or update the .LStable file in the specified directory.

    Args:
        directory (str): Path to the directory containing the .LStable file.
        mappings (dict): Dictionary of {dbidL: dbidS} mappings to include.
    """
    lstable_path = os.path.join(directory, ".LStable")

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    # Write mappings to .LStable
    with open(lstable_path, "w") as f:
        for dbidL, dbidS in mappings.items():
            f.write(f"{dbidL}={dbidS}\n")

    print(f".LStable file updated at {lstable_path}")

# Example Usage
directory = "dd_data/UserData"
mappings = {
    "UserData": "001",
    "TransactionLogs": "002"
}
create_or_update_lstable(directory, mappings)
```

\#### Validation Script

A script to validate [.LStable]{.title-ref} files ensures compliance
with standards:

``` python
def validate_lstable(directory):
    """
    Validate the .LStable file in the specified directory.

    Args:
        directory (str): Path to the directory containing the .LStable file.
    """
    lstable_path = os.path.join(directory, ".LStable")

    if not os.path.exists(lstable_path):
        raise FileNotFoundError(f".LStable file not found in {directory}")

    with open(lstable_path, "r") as f:
        for line in f:
            if "=" not in line:
                raise ValueError(f"Invalid entry in .LStable: {line.strip()}")

    print(f".LStable in {directory} is valid.")

# Example Usage
validate_lstable("dd_data/UserData")
```

\### 5. Key Benefits of Standardization

1.  **Simplicity:** Centralized and uniform handling of mappings reduces
    complexity.
2.  **Reusability:** Consistent [.LStable]{.title-ref} usage ensures
    that tools and utilities can operate across all directories.
3.  **Error Reduction:** Validations prevent misconfigurations or
    identifier conflicts.
4.  **Scalability:** Supports large systems by enabling automated tools
    to read and manage mappings efficiently.

\### 6. Best Practices

-   **Automation:** Use scripts to create, update, and validate
    [.LStable]{.title-ref} files.
-   **Version Control:** Track changes to [.LStable]{.title-ref} files
    using version control systems.
-   **Documentation:** Include a README in each [dd_data]{.title-ref}
    directory to explain the [.LStable]{.title-ref} file's purpose and
    structure.
-   **Regular Audits:** Periodically validate [.LStable]{.title-ref}
    files to ensure compliance and detect inconsistencies.

## Configuration Files: Creating Abstract Objects and Control Planes

The configuration files in the Dimensional Directory project enable the
creation of abstract objects and control planes, which are foundational
to building data structures called **AddressPlanes**. These
AddressPlanes are hierarchical mappings of data that facilitate
modularity and scalability.

This section explains the structure and usage of the configuration files
([.dddoc]{.title-ref}, [.ddapc]{.title-ref}, and [.ddopm]{.title-ref})
and demonstrates how they interact to define and manage abstract objects
and AddressPlanes.

### Configuration Files Overview

\### 1. Data Object Configuration ([.dddoc]{.title-ref})

The [.dddoc]{.title-ref} files define the structure of data objects,
including their fields and data types.

**Example \`.dddoc\` File:**

``` json
{
  "DataObjectConfig": {
    "model_name": "ExampleDataObject",
    "fields": [
      {"name": "uuid", "type": "UUID", "required": true},
      {"name": "name", "type": "String", "required": true},
      {"name": "value", "type": "Float", "required": true}
    ]
  }
}
```

**Key Elements:**

-   **model_name**: The name of the data object model.
-   **fields**: A list of field definitions, including:
    -   \`name\`: The field name.
    -   \`type\`: The data type of the field.
    -   \`required\`: Whether the field is mandatory.

\### 2. Address Plane Configuration ([.ddapc]{.title-ref})

The [.ddapc]{.title-ref} files define the structure of AddressPlanes,
specifying their hierarchical levels.

**Example \`.ddapc\` File:**

``` json
{
  "AddressPlane": {
    "plane_id_L": "ExamplePlane",
    "plane_id_S": "001",
    "columns": [
      {"col_name": "uuid", "col_type": "UUIDSelector"},
      {"col_name": "name", "col_type": "String"},
      {"col_name": "value", "col_type": "Float"}
    ]
  }
}
```

**Key Elements:**

-   **plane_id_L**: The long identifier for the plane.
-   **plane_id_S**: The short identifier for the plane.
-   **columns**: A list of columns in the AddressPlane, including:
    -   \`col_name\`: The column name.
    -   \`col_type\`: The type of data stored in the column.

\### 3. Object to Plane Mapping ([.ddopm]{.title-ref})

The [.ddopm]{.title-ref} files map data objects to AddressPlanes by
linking object fields to AddressPlane columns.

**Example \`.ddopm\` File:**

``` json
{
  "ObjectPlaneMapping": {
    "source_model": "ExampleDataObject",
    "target_plane": "ExamplePlane",
    "field_to_column": {
      "uuid": "uuid",
      "name": "name",
      "value": "value"
    }
  }
}
```

**Key Elements:**

-   **source_model**: The name of the data object.
-   **target_plane**: The name of the AddressPlane.
-   **field_to_column**: A dictionary mapping object fields to plane
    columns.

## How Config Files Work Together

The [.dddoc]{.title-ref}, [.ddapc]{.title-ref}, and [.ddopm]{.title-ref}
files are used collectively to create and manage AddressPlanes.

\### Step-by-Step Process

\#### 1. Define the Data Object ([.dddoc]{.title-ref})

Start by creating a [.dddoc]{.title-ref} file to define the structure of
the data object. For instance, the [ExampleDataObject]{.title-ref}
defined above has [uuid]{.title-ref}, [name]{.title-ref}, and
[value]{.title-ref} fields.

\#### 2. Define the Address Plane ([.ddapc]{.title-ref})

Next, create a [.ddapc]{.title-ref} file to define the AddressPlane.
Specify the plane\'s identifier and the columns corresponding to the
data object fields.

\#### 3. Map the Object to the Plane ([.ddopm]{.title-ref})

Finally, create a [.ddopm]{.title-ref} file to map the data object
fields to the AddressPlane columns. This file links the abstract object
to its control plane.

## Example Usage: Building AddressPlanes

The following components of the project utilize these configuration
files to build and manage AddressPlanes:

\### 1. DDConfigBuilder

The [DDConfigBuilder]{.title-ref} class is used to create configuration
files programmatically.

**Example Code:**

``` python
from core.manager.pmm.dd_config_builder import DDConfigBuilder

# Initialize Config Builder
builder = DDConfigBuilder()

# Create a Data Object Configuration
builder.build_dddoc("example_object", {
    "uuid": "UUID",
    "name": "String",
    "value": "Float"
})

# Create an Address Plane Configuration
builder.build_ddapc("example_plane", ["level1", "level2"])

# Create an Object to Plane Mapping
builder.build_ddopm("example_object", "example_plane", {
    "uuid": "level1.uuid",
    "name": "level1.name",
    "value": "level2.value"
})
```

\### 2. ObjectMapper

The [ObjectMapper]{.title-ref} class loads configurations and retrieves
mappings for data objects and AddressPlanes.

**Example Code:**

``` python
from core.manager.pmm.object_mapper import ObjectMapper

# Initialize ObjectMapper
mapper = ObjectMapper(config_dir="configs")

# Retrieve Address Mapping
mapping = mapper.get_address_mapping("example_object")
print(mapping)

# Retrieve Plane Hierarchy
hierarchy = mapper.get_plane_hierarchy("example_plane")
print(hierarchy)
```

\### 3. DataObjectCRUD

The [DataObjectCRUD]{.title-ref} class uses the
[ObjectMapper]{.title-ref} and [DatabaseWrapper]{.title-ref} to perform
CRUD operations on AddressPlanes.

**Example Code:**

``` python
from core.manager.pmm.data_object_crud import DataObjectCRUD
from core.manager.dd_manager.json_database_wrapper import JSONDatabaseWrapper

# Initialize CRUD and Database Wrapper
crud = DataObjectCRUD(mapper, JSONDatabaseWrapper(base_path="dd_data"))

# Create a Data Object
crud.create("example_object", {
    "uuid": "123e4567-e89b-12d3-a456-426614174000",
    "name": "Example",
    "value": 42.0
})

# Read the Data Object
data = crud.read("example_object")
print(data)
```

## Benefits of Configuration Files

\### 1. Centralized Management

Simplifies the tracking and management of data object and AddressPlane
configurations.

\### 2. Reusability

Ensures that configuration patterns can be reused across different parts
of the system, enhancing consistency.

\### 3. Error Reduction

Automated validations and standardized formats prevent misconfigurations
and ensure data integrity.

\### 4. Scalability

Facilitates the management of large and complex systems by enabling
automated and consistent mapping through configuration files.

## Best Practices

1.  **Automate Creation:** Use scripts and tools to generate
    configuration files to maintain consistency.
2.  **Track Changes:** Utilize version control systems (e.g., Git) to
    monitor changes to configuration files.
3.  **Document Purpose:** Include documentation (e.g., README files) in
    each configuration directory explaining the purpose and structure of
    the files.
4.  **Regular Validation:** Periodically validate configuration files to
    ensure they adhere to defined standards and mappings are correct.

## Conclusion

By integrating **L and S identifiers**, the standardized
**\`.LStable\`** file, and well-defined configuration files
([.dddoc]{.title-ref}, [.ddapc]{.title-ref}, and [.ddopm]{.title-ref}),
the Dimensional Directory framework achieves a robust, modular, and
scalable approach to data management. These components work
synergistically to ensure clarity, consistency, and efficiency, laying a
solid foundation for future-proof system design.

Adhering to the outlined best practices and leveraging the provided
scripts and examples will further enhance the maintainability and
scalability of your data management systems within the Dimensional
Directory framework.
