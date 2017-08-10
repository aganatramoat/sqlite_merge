/***************** start of mergedbs.c **************************************/

typedef struct MDHandle
{
    sqlite3 *db;
    sqlite_int64 rootpage;   // column rootpage in sqlite_master
    BtCursor *cursor;
} MDHandle;


typedef struct MDTab
{
    const char *reltype;     // either table or index
    char *relname;           // column name in sqlite_master
    MDHandle src;
    MDHandle dest;
    struct MDTab *next;     // make a linked list
} MDTab;


SQLITE_API int md_mergedbs(const char *src, const char *dest,
                           char *errstr, int errlen);
static int md_cpdefs(sqlite3 *src, sqlite3 *dest, const char *reltype, 
                     MDTab **tablist);
static int md_cprels(MDTab *tablist);
static MDTab *md_tabCons(sqlite3 *src, sqlite3 *dest, char const *reltype, 
                         unsigned char const *relname, 
                         sqlite_int64 src_rootpage);
static void md_freeTabs(MDTab **head);
static void md_insertTab(MDTab **head, MDTab *newelement);
static void md_printTabs(MDTab *head);
static int md_createTable(unsigned char const *creation_sql, MDTab *tab);
static int md_cprows(MDTab *tab);
static void md_closeCursors(MDTab *tab);
static void md_closeCursor(BtCursor **cursor);
static KeyInfo * md_getKeyInfo(sqlite3 *db, char const *zName);
static int md_openCursors(MDTab *tab);
static int md_openCursor(char const *reltype, char const *relname, 
                         int flag, MDHandle *handle);
static int md_beginTrans(MDTab *tab);
static int md_endTrans(MDTab *, int commit);


/*
**   Replicate the tables/indexes in src file to the dest file
**   return SQLITE_OK on success, some other code on failure
**   Errors will be reported in errstr, pass in 0 if not required
*/
SQLITE_API int md_mergedbs(const char *src, const char *dest,
                           char *errstr, int errlen)
{
    sqlite3 *srcdb = NULL;
    sqlite3 *destdb = NULL;
    int rc = 0;
    MDTab *tablist = NULL;

    rc = sqlite3_open_v2(src, &srcdb, SQLITE_OPEN_READONLY, 0);
    if ( rc != SQLITE_OK)
        goto cleanup;
    
    rc = sqlite3_open_v2(dest, &destdb, 
                         SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 0);
    if ( rc != SQLITE_OK)
        goto cleanup;
    
    // create the tables in destdb.
    rc = md_cpdefs(srcdb, destdb, "table", &tablist);
    if ( rc != SQLITE_OK)
        goto cleanup;
    
    // create the indices in destdb.
    rc = md_cpdefs(srcdb, destdb, "index", &tablist);
    if ( rc != SQLITE_OK)
        goto cleanup;
    
    // cp the relations from src to dest
    if (tablist)
        rc = md_cprels(tablist);

cleanup:
    if ( rc != SQLITE_OK && errstr )
        snprintf(errstr, errlen,
             "Error from source db: %s\n"
             "Error from dest db: %s", 
             sqlite3_errmsg(srcdb), sqlite3_errmsg(destdb));
    sqlite3_close_v2(srcdb);
    sqlite3_close_v2(destdb);
    md_freeTabs(&tablist);
    return rc;
}


/*
 *   Constructor for MDTab
 *   The dest.rootpage will be populated after the table is created
 *   in the destination database
 *   Called by md_cpdefs
 */
static MDTab *
md_tabCons(sqlite3 *src, sqlite3 *dest, char const *reltype, 
           unsigned char const *relname, sqlite_int64 src_rootpage)
{
    MDTab *ret = (MDTab *)sqlite3_malloc(sizeof(MDTab));
    
    memset(ret, 0, sizeof(MDTab));
    ret->reltype = reltype;
    ret->relname = sqlite3DbStrDup(0, (char const *)relname);
    ret->src.db = src;
    ret->src.rootpage = src_rootpage;
    ret->dest.db = dest;

    return ret;
}


/*
**  Append new MDTab at the end of the list
*/
static void
md_insertTab(MDTab **head, MDTab *newelement)
{
    MDTab *iter = *head;

    if (iter == NULL) {
        *head = newelement;
        return;
    }
    while (iter->next)
        iter = iter->next;
    iter->next = newelement;
}


/*
**  Destructor for a list of MDTab
*/
static void
md_freeTabs(MDTab **head)
{
    MDTab *ele = *head;
    MDTab *temp = NULL;
    
    while (ele) {
        temp = ele->next;
        sqlite3_free(ele->relname);
        sqlite3_free(ele);
        ele = temp;
    }
    *head = NULL;
}


static void
md_printTabs(MDTab *head)
{
    while (head) {
        TRACE(("RelName: %s, SrcRoot: %lld, DestRoot: %lld\n",
                head->relname, head->src.rootpage, 
                head->dest.rootpage));
        head = head->next;
    }
}


/*
 *  Create the table/index in the destination database
 *  The rootpage of the created relation is stored in tab->dest.rootpage
 */
static int
md_createTable(unsigned char const *creation_sql, MDTab *tab)
{
    sqlite3 *db = tab->dest.db;
    sqlite3_stmt *stmt = NULL;
    char *rootpage_q = NULL;
    int rc = execSql(db, NULL, (char const *)creation_sql);
    
    if ( rc != SQLITE_OK )
        goto cleanup;

    rootpage_q = (char *)sqlite3_malloc(strlen(tab->relname) + 256);
    sprintf(rootpage_q, "select rootpage from sqlite_master where "
                        "name = '%s'", tab->relname);
    rc = sqlite3_prepare_v2(db, rootpage_q, -1, &stmt, NULL);
    if ( rc != SQLITE_OK )
        goto cleanup;

    if ( sqlite3_step(stmt) == SQLITE_ROW )
        tab->dest.rootpage = sqlite3_column_int64(stmt, 0);
    else
        rc = SQLITE_ERROR;

cleanup:
    if (stmt)
        sqlite3_finalize(stmt);
    if (rootpage_q)
        sqlite3_free(rootpage_q);
    return rc;
}


/*
**   reltype is either table or index
**   Find all the tables of reltype in src db and create them in dest db. 
**   The MDTab list is extended 
*/
static int
md_cpdefs(sqlite3 *src, sqlite3 *dest, const char *reltype, MDTab **tablist)
{
    char srcq[256];
    sqlite3_stmt *stmt = NULL;
    int rc;

    snprintf(srcq, 256, "select name, rootpage, sql from sqlite_master "
                        "where type = '%s' and "
                        "name not like 'sqlite%%'", reltype);

    rc = sqlite3_prepare_v2(src, srcq, -1, &stmt, NULL);
    if ( rc != SQLITE_OK )
        goto cleanup;
    
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        MDTab *tab = md_tabCons(src, dest, reltype,  
                                sqlite3_column_text(stmt, 0),
                                sqlite3_column_int64(stmt, 1));
        rc = md_createTable(sqlite3_column_text(stmt, 2), tab);
        if ( rc != SQLITE_OK )
            goto cleanup;
        md_insertTab(tablist, tab);
    }

cleanup:
    if ( stmt )
        sqlite3_finalize(stmt);
    return rc;
}


/*
 * Copying over some comments to help read the code
** For a table btree (used for rowid tables), only the pX.nKey value of
** the key is used. The pX.pKey value must be NULL.  The pX.nKey is the
** rowid or INTEGER PRIMARY KEY of the row.  The pX.nData,pData,nZero fields
** hold the content of the row.
**
** For an index btree (used for indexes and WITHOUT ROWID tables), the
** key is an arbitrary byte sequence stored in pX.pKey,nKey.  The 
** pX.pData,nData,nZero fields must be zero.
*/
static void
md_fillPayload(CellInfo *cellinfo, int isindex, BtreePayload *payload)
{
    memset(payload, 0, sizeof(BtreePayload));
    if (isindex) {
        payload->nKey = cellinfo->nPayload;
        payload->pKey = (const void *)cellinfo->pPayload;
    }
    else {
        payload->nKey = cellinfo->nKey;
        payload->nData = cellinfo->nPayload;
        payload->pData = (const void *)cellinfo->pPayload;
    }
}


/*
**  Open cursors for the source and destination
**  Iterate over the source cursor and push data to the destination cursor
*/
static int
md_cprows(MDTab *tab)
{
    BtCursor *src;
    BtCursor *dest;
    CellInfo *cellinfo;
    BtreePayload payload;
    int pRes = 0;
    int rc = md_openCursors(tab);
    
    if ( rc != SQLITE_OK )
        goto cleanup;

    src = tab->src.cursor;
    dest = tab->dest.cursor;
    cellinfo = &src->info;

    rc = sqlite3BtreeFirst(src, &pRes);
    if ( rc != SQLITE_OK )
        goto cleanup;

    while ( ! sqlite3BtreeEof(src) ) {
        rc = sqlite3BtreeLast(dest, &pRes);
        if ( rc != SQLITE_OK )
            break;
        getCellInfo(src);
        md_fillPayload(cellinfo, src->pKeyInfo != NULL, &payload);
        rc = sqlite3BtreeInsert(dest, &payload, 
                                BTREE_APPEND | BTREE_SAVEPOSITION, -1);
        if ( rc != SQLITE_OK )
            break;
        rc = sqlite3BtreeNext(src, &pRes);
        if ( rc != SQLITE_OK )
            break;
    }
    
cleanup:
    md_closeCursors(tab);
    return rc;
}


static void
md_closeCursor(BtCursor **caddr)
{
    BtCursor *cursor = *caddr;
    if (cursor) {
        sqlite3BtreeCloseCursor(cursor);
        sqlite3_free(cursor);
        *caddr = NULL;
    }
}


static void
md_closeCursors(MDTab *tab)
{
    md_closeCursor(&tab->src.cursor);
    md_closeCursor(&tab->dest.cursor);
}


/*
**  zName is the name of an index
*/
static KeyInfo *
md_getKeyInfo(sqlite3 *db, char const *zName)
{
    Schema *pSchema = db->aDb[0].pSchema;
    Index *pIndex = sqlite3HashFind(&pSchema->idxHash, zName);
    Parse parse;

    memset(&parse, 0, sizeof(Parse));
    parse.db = db;
    return sqlite3KeyInfoOfIndex(&parse, pIndex);
}

    
static int
md_openCursor(char const *reltype, char const *relname, 
              int flag, MDHandle *handle)
{
    sqlite3 *db = handle->db;
    Btree *tree = db->aDb[0].pBt;
    handle->cursor = (BtCursor *)sqlite3_malloc(sizeof(BtCursor));
    
    sqlite3BtreeCursorZero(handle->cursor);
    return sqlite3BtreeCursor(
        tree, handle->rootpage,
        flag,
        strcmp(reltype, "index") == 0 ? md_getKeyInfo(db, relname) : 0,
        handle->cursor);
}


/*
**  Create a cursor for the source and destination
*/
static int
md_openCursors(MDTab *tab)
{
    char const *reltype = tab->reltype;
    char const *relname = tab->relname;
    int rc = md_openCursor(reltype, relname, 0, &tab->src);
    
    if ( rc == SQLITE_OK )
        rc = md_openCursor(reltype, relname, BTREE_WRCSR, &tab->dest);

    if ( rc != SQLITE_OK )
        md_closeCursors(tab);
    return rc;
}


/*
** Loop through the list tab and copy each table from 
** src to dest
** Called by the main function md_mergedbs
** md_cprows call below does the actual copying
*/
static int
md_cprels(MDTab *tablist)
{
    MDTab *tab = tablist;
    int rc = md_beginTrans(tablist);

    if ( rc != SQLITE_OK )
        return rc;
    
    while (tab) {
        rc = md_cprows(tab);
        if ( rc != SQLITE_OK )
            break;
        tab = tab->next;
    }
    
    if ( rc == SQLITE_OK )
        return md_endTrans(tablist, 1);
    else {
        md_endTrans(tablist, 0);
        return rc;
    }
}

/*
**  Begin a transction for both src and dest
*/
static int
md_beginTrans(MDTab *tab)
{
    sqlite3 *src = tab->src.db;
    sqlite3 *dest = tab->dest.db;
    Btree *srctree = src->aDb[0].pBt;
    Btree *desttree = dest->aDb[0].pBt;
    int rc = 0;

    if ( srctree->inTrans != TRANS_NONE )
        return SQLITE_ERROR;
    
    if ( desttree->inTrans != TRANS_NONE )
        return SQLITE_ERROR;
    
    sqlite3_mutex_enter(src->mutex);
    rc = sqlite3BtreeBeginTrans(srctree, 0);
    if ( rc != SQLITE_OK ) {
        sqlite3_mutex_leave(src->mutex);
        return rc;
    }
    
    sqlite3_mutex_enter(dest->mutex);
    rc = sqlite3BtreeBeginTrans(desttree, 2);
    if ( rc != SQLITE_OK ) {
        sqlite3_mutex_leave(src->mutex);
        sqlite3_mutex_leave(dest->mutex);
        sqlite3BtreeCommit(srctree);
        return rc;
    }

    return SQLITE_OK;
}


/*
**  Commit or rollback
*/
static int
md_endTrans(MDTab *tab, int commit)
{
    sqlite3 *src = tab->src.db;
    sqlite3 *dest = tab->dest.db;
    Btree *srctree = src->aDb[0].pBt;
    Btree *desttree = dest->aDb[0].pBt;
    int rc = 0;
    
    sqlite3_mutex_leave(src->mutex);
    sqlite3_mutex_leave(dest->mutex);
    
    sqlite3BtreeCommit(srctree);
    if ( commit ) {
        rc = sqlite3BtreeCommit(desttree);
        if ( rc != SQLITE_OK )
            sqlite3BtreeRollback(desttree, SQLITE_OK, 0);
    }
    else
        rc = sqlite3BtreeRollback(desttree, SQLITE_OK, 0);

    return rc;
}

/***************** end of mergedbs.c **************************************/
