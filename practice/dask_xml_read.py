import dask.bag as db
import dask.dataframe as dd
from lxml import etree
import xmlschema
import pandas as pd

# Define file paths
XML_FILE = 'large_file.xml'
XSD_FILE = 'schema_file.xsd'

# Step 1: Validate XML against XSD schema (Optional but Recommended)
schema = xmlschema.XMLSchema(XSD_FILE)
if not schema.is_valid(XML_FILE):
    print("XML validation failed!")
    exit()

# Step 2: Define a function to parse a single XML chunk
def parse_xml_chunk(chunk):
    try:
        # Parse the chunk using lxml
        root = etree.fromstring(chunk)
        # Extract data (assuming a flat XML structure for simplicity)
        data = {child.tag: child.text for child in root}
        return data
    except etree.XMLSyntaxError:
        return None

# Step 3: Stream and split XML file into manageable chunks
def xml_chunk_generator(file_path, element_tag):
    context = etree.iterparse(file_path, events=("end",), tag=element_tag, huge_tree=True)
    for event, elem in context:
        yield etree.tostring(elem, encoding='utf-8')
        # Clear element to save memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

# Step 4: Use Dask Bag to parallelize XML parsing
element_tag = 'your_element_tag'  # Replace with the main XML element tag to parse
xml_chunks = db.from_sequence(xml_chunk_generator(XML_FILE, element_tag), npartitions=200)
parsed_bag = xml_chunks.map(parse_xml_chunk).filter(lambda x: x is not None)

# Step 5: Convert parsed data to Dask DataFrame
df = parsed_bag.to_dataframe()

# Trigger computation and print sample rows
print(df.head())
