import { SchemaBuilder, Index, Document } from '@oxdev03/node-tantivy-binding'
import * as os from 'os'
import * as path from 'path'
import * as fs from 'fs'

const schemaBuilder = new SchemaBuilder()
schemaBuilder.addTextField('id', { stored: true })
schemaBuilder.addJsonField('data', { stored: false })
const schema = schemaBuilder.build()
fs.mkdirSync('./data/index', { recursive: true })
const index = new Index(schema, './data/index')

function addDocument(id, data) {
    const doc = new Document()
    doc.addText('id', id)
    doc.addJson('data', data)

    const writer = index.writer()
    writer.addDocument(doc)
    writer.commit()
    writer.waitMergingThreads()
}

function searchDocuments(query) {
    const searchQuery = index.parseQuery(query)
    const searcher = index.searcher()
    const searchResults = searcher.search(searchQuery)
    const results = searchResults.hits.map(hit => {
        const doc = searcher.doc(hit.docAddress)
        return doc.toDict().id[0]
    })

    return results
}

export { addDocument, searchDocuments }