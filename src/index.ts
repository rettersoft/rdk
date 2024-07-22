import { AsyncLocalStorage } from 'async_hooks';
import axios from 'axios'
export interface KeyValue {
    [key: string]: any
}
export interface KeyValueString {
    [key: string]: string
}

export interface Configuration {
    stepLimit?: number
}

export interface OperationResponse {
    success: boolean
    data?: any
    error?: string
}
export interface GetFileResponse extends OperationResponse {
    extraData?: {
        url: string
    }
}

export interface GenerateCustomTokenResponse extends OperationResponse {
    data?: {
        customToken: string
    }
}
export interface InvalidateCacheResponse extends OperationResponse {
    data?: {
        id: string
    }
}
export interface CloudObjectResponse<T = any> {
    statusCode: number
    body?: T
    headers?: KeyValueString
    retryAfter?: number
}

export interface Response<T = any> extends CloudObjectResponse<T> {
    isBase64Encoded?: boolean
    retryAfter?: number
}

export interface Request<T = any> {
    httpMethod: string
    body?: T
    headers: KeyValueString
    queryStringParams: Record<string, any>
    pathParameters?: {
        path: string
        rule?: string
        params?: KeyValueString
    }
}

export interface Context {
    requestId: string
    projectId: string
    action: string
    identity: string
    serviceId?: string
    headers?: KeyValue
    classId: string
    instanceId?: string
    methodName: string
    refererClassId?: string
    refererInstanceId?: string
    refererMethodName?: string
    refererUserId?: string
    refererIdentity?: string
    claims?: KeyValue
    isAnonymous?: boolean
    culture?: string
    platform?: string
    userId?: string
    customProjectId?: string
    sourceIP: string
    sessionId?: string
    clientOs?: string
    targetServiceIds?: string[]
    relatedUserId?: string
}

type UserState = { [userId: string]: { [key: string]: any } }
type RoleState = { [identity: string]: { [key: string]: any } }

export interface State<PUB = KeyValue, PRIV = KeyValue, USER = UserState, ROLE = RoleState> {
    public?: PUB
    private?: PRIV
    user?: USER
    role?: ROLE
}

export interface Task {
    classId?: string
    instanceId?: string
    lookupKey?: {
        name: string
        value: string
    }
    payload?: any
    method: string
    after: number
    retryConfig?: RetryConfig
}
export interface GetMemory {
    key: string
}
export interface DeleteMemory {
    key: string
}

export interface SetMemory {
    key: string
    value: any
    expireAt?: number
}
export interface IncrementMemory {
    key: string
    path?: string
    value: number
}
export interface GetFile {
    filename: string
    returnSignedURL?: boolean
}
export interface DeployProject {
    force?: boolean
}
export interface InvalidateCache {
    classId?: string
    methodName?: string
    instanceId?: string
}
export type Architecture = 'arm64' | 'x86_64' | 'x86_64,arm64' | 'arm64,x86_64'
export type Runtime = 'nodejs12.x,nodejs14.x' | 'nodejs12.x' | 'nodejs14.x' | 'nodejs16.x' | 'nodejs20.x' | 'nodejs14.x,nodejs12.x'
export interface UpsertDependency {
    dependencyName: string
    zipFile: Buffer
    architectures?: Architecture
    runtimes?: Runtime
}

export interface DeleteDependency {
    dependencyName: string
}

export interface SetFile {
    filename: string
    body: string
}
export interface SetFileOperation {
    filename: string
    body?: string
    size: number
    large: boolean
}
export interface LookUpKey {
    key: {
        name: string
        value: string
    }
}

export interface GetLookUpKey extends LookUpKey {
    classId?: string
}

/**
 * lookupKey or instanceId must be given
 *
 * @export
 * @interface GetInstance
 */
export interface GetInstance {
    httpMethod?: string
    queryStringParams?: Record<string, any>
    headers?: KeyValueString
    body?: any
    classId: string
    instanceId?: string
    lookupKey?: {
        name: string
        value: string
    }
}

export interface ListFiles {
    prefix?: string
    nextToken?: string
}

export interface ListInstanceIds {
    classId?: string
    nextToken?: string
    instanceIdPrefix?: string
}

export interface RetryConfig {
    delay: number
    count: number
    rate: number
}

export interface MethodCall extends GetInstance {
    methodName: string
    retryConfig?: RetryConfig,
}

export interface BulkImport {
    getInstance?: GetInstance[],
    methodCall?: MethodCall[],
}

export interface GenerateCustomToken {
    userId: string
    identity: string
    claims?: KeyValue
}

export interface TerminateSession {
    userId: string
    identity?: string
}

export interface DeleteClass {
    classId: string
}

export interface DeleteAllInstances {
    classId: string
}

export interface DeleteInstance extends DeleteClass {
    instanceId: string
}

export interface ReadOnlyOperationsInput {
    getMemory?: GetMemory[]
    readDatabase?: ReadDatabase[]
    queryDatabase?: QueryDatabase[]
    getFile?: GetFile[]
    getLookUpKey?: GetLookUpKey[]
    bulkImport?: BulkImport[]
    methodCall?: MethodCall[]
    getInstance?: GetInstance[]
    listInstanceIds?: ListInstanceIds[]
    listFiles?: ListFiles[]
    getState?: GetInstance[]
    generateCustomToken?: GenerateCustomToken[]
    httpRequest?: StaticIPRequest[]
}

// * <database>
export interface ReadDatabase {
    partKey: string
    sortKey: string
    memory?: boolean
}
export interface WriteToDatabase {
    partKey: string
    sortKey: string
    memory?: boolean
    expireAt?: number
    data: Record<string, unknown>
}
export interface IncrementDatabase {
    partKey: string
    sortKey: string
    path?: string
    value: number
    memory?: boolean
    expireAt?: number,
}
export interface RemoveFromDatabase {
    partKey: string
    sortKey: string
}
export interface RemovePartitionFromDatabase {
    partKey: string
}
export interface QueryDatabase {
    partKey: string
    beginsWith?: string
    greaterOrEqual?: string
    lessOrEqual?: string
    reverse?: boolean
    nextToken?: string
    limit?: number
}
// * </database>

export enum StaticIPHttpMethod {
    'get' = 'get',
    'GET' = 'GET',
    'delete' = 'delete',
    'DELETE' = 'DELETE',
    'head' = 'head',
    'HEAD' = 'HEAD',
    'options' = 'options',
    'OPTIONS' = 'OPTIONS',
    'post' = 'post',
    'POST' = 'POST',
    'put' = 'put',
    'PUT' = 'PUT',
    'patch' = 'patch',
    'PATCH' = 'PATCH',
    'purge' = 'purge',
    'PURGE' = 'PURGE',
    'link' = 'link',
    'LINK' = 'LINK',
    'unlink' = 'unlink',
    'UNLINK' = 'UNLINK',
}
export interface StaticIPCallback {
    projectId: string
    classId: string
    instanceId?: string
    methodName: string
}
export interface StaticIPRequest {
    url: string
    data: {
        requestData?: any
        returnData?: any
        returnEndpoint?: StaticIPCallback
    }
    headers?: Record<string, string>
    method?: StaticIPHttpMethod
    timeout?: number
    sync?: boolean
    auth?: {
        username: string
        password: string
    }
    disableSSL?: boolean
    config?: Record<string, any>
}

export interface ReadDatabaseResponse extends OperationResponse {
    data?: { part: string, sort: string, data?: any }
}

export type DatabaseOutput = { partKey: string, sortKey: string, data?: any }
export interface QueryDatabaseResponse extends OperationResponse {
    data?: { items?: DatabaseOutput[], nextToken?: string }
}

export interface GetLookupKeyResponse extends OperationResponse {
    data?: { instanceId: string }
}

export interface ListInstanceIdsResponse extends OperationResponse {
    data?: { instanceIds: string[], nextToken?: string }
}

export interface ListFilesResponse extends OperationResponse {
    data?: { files: string[], nextToken?: string }
}

export interface BulkImportResponse extends OperationResponse {
    data?: { execution: string, startDate?: string }
}

export interface MethodDefinitionCommonModels { inputModel?: string, queryStringModel?: string }
export interface MethodDefinitionSummary extends MethodDefinitionCommonModels {
    name: string,
    type: string,
    outputModel?: string,
    errorModel?: string,
}

export interface GetInstanceResponse extends CloudObjectResponse {
    body?: {
        newInstance: boolean
        instanceId: string
        init?: MethodDefinitionCommonModels
        get?: MethodDefinitionCommonModels
        methods: MethodDefinitionSummary[],
        response?: any
    }
}

export interface OperationsInput extends ReadOnlyOperationsInput {
    setMemory?: SetMemory[]
    deleteMemory?: GetMemory[]
    incrementMemory?: IncrementMemory[]
    writeToDatabase?: WriteToDatabase[]
    incrementDatabase?: IncrementDatabase[]
    removeFromDatabase?: RemoveFromDatabase[]
    setFile?: (SetFile | SetFileOperation)[]
    deleteFile?: GetFile[]
    setLookUpKey?: LookUpKey[]
    deleteLookUpKey?: LookUpKey[]
    upsertDependency?: UpsertDependency[]
    deleteDependency?: DeleteDependency[]
    deployProject?: DeployProject[]
    invalidateCache?: InvalidateCache[]
    terminateSession?: TerminateSession[]
    deleteInstance?: DeleteInstance[]
    deleteAllInstances?: DeleteAllInstances[]
    deleteClass?: DeleteClass[]
}

export interface ReadonlyOperationsOutput {
    getMemory?: OperationResponse[]
    readDatabase?: ReadDatabaseResponse[]
    queryDatabase?: QueryDatabaseResponse[]
    getFile?: GetFileResponse[]
    getLookUpKey?: GetLookupKeyResponse[]
    bulkImport?: BulkImportResponse[]
    methodCall?: CloudObjectResponse[]
    getInstance?: GetInstanceResponse[]
    listInstanceIds?: ListInstanceIdsResponse[]
    listFiles?: ListFilesResponse[]
    getState?: CloudObjectResponse[]
    generateCustomToken?: GenerateCustomTokenResponse[]
    httpRequest?: OperationResponse[],
}

export interface OperationsOutput extends ReadonlyOperationsOutput {
    setMemory?: OperationResponse[]
    deleteMemory?: OperationResponse[]
    incrementMemory?: OperationResponse[]
    writeToDatabase?: OperationResponse[]
    incrementDatabase?: OperationResponse[]
    removeFromDatabase?: OperationResponse[]
    setFile?: OperationResponse[]
    deleteFile?: OperationResponse[]
    setLookUpKey?: OperationResponse[]
    deleteLookUpKey?: OperationResponse[]
    upsertDependency?: OperationResponse[]
    deleteDependency?: OperationResponse[]
    deployProject?: OperationResponse[]
    invalidateCache?: InvalidateCacheResponse[]
    terminateSession?: OperationResponse[]
    deleteInstance?: CloudObjectResponse[]
    deleteAllInstances?: OperationResponse[]
    deleteClass?: CloudObjectResponse[]
}

export interface RioEvent {
    name: string
    payload: Record<string, any>
}

export interface Data<I = any, O = any, PUB = KeyValue, PRIV = KeyValue, USER = UserState, ROLE = RoleState> {
    context: Context
    env: KeyValue
    config: Configuration
    version: number
    state: State<PUB, PRIV, USER, ROLE>
    request: Request<I>
    response: Response<O>
    tasks: Task[]
    events: RioEvent[]
}

let rdkUrl: string | undefined
let context: Context | undefined
let level: number | undefined

const fileSizeLimit = 250000000


// let operationCountMilestone = 0;
// let concurrentLambdaCountLimit = 0;

// let usageCheckCounter = 0;
let operationCount = 0
// let concurrentLambdaCount = 0;
let asyncContext :AsyncLocalStorage<Context> | null

export function init(params: { url: string, asyncContext: AsyncLocalStorage<Context> }) {
    rdkUrl = params.url
    asyncContext = params.asyncContext
    // operationCountMilestone = params.ocLimit || 100
    // concurrentLambdaCountLimit = params.clcLimit || 10
}
function getAsyncContext() {
    if (!asyncContext) throw new Error('Async context is not initialized')

    return asyncContext.getStore()
}

function calculateSize(data: string) {
    return new TextEncoder().encode(data).length
}
async function callOperationApi(payload: OperationsInput): Promise<OperationsOutput | Error> {
    // if (concurrentLambdaCount > concurrentLambdaCountLimit) {
    //     throw new Error(`Cannot send more than ${concurrentLambdaCountLimit} operations without pipeline`);
    // }

    operationCount = Object.values(payload).reduce((total, op) => total + op.length, operationCount)
    // const newUsageCheck = Math.floor(operationCount / operationCountMilestone)
    // let checkUsage = false
    // if (newUsageCheck > 0 && newUsageCheck !== usageCheckCounter) {
    //     usageCheckCounter = newUsageCheck
    //     checkUsage = true
    // }

    console.log("getAsyncContext", getAsyncContext())

    // concurrentLambdaCount++
    // TODO! custom httpAgent?
    // todo DELETE LEVEL !!!
    return axios.post(rdkUrl!, { context: getAsyncContext(), level: 1, input: { data: payload, rdkVersion: '2.0.0' } })
        .then(({ data }) => {
            const message = data.error || data.limitError
            if (message) return new Error(message)

            delete data?.limitError
            delete data?.error
            // concurrentLambdaCount--
            return data as OperationsOutput
        })
        .catch((e) => e)
}

export default class CloudObjectsOperator {

    /**
     *
     * Creates a pipeline which gathers operations until the send method is called, and then sends a batch request for all of them.
     * @memberof CloudObjectsOperator
     */
    pipeline(): CloudObjectsPipeline {
        return new CloudObjectsPipeline()
    }

    private async sendSingleOperation(input: any, operationType: string) {
        return callOperationApi({ [operationType]: [input] }).then((r) => {
            if (r instanceof Error) return { success: false, error: r.message }

            return r[operationType]?.pop()
        })
    }

    async deleteInstance(input: DeleteInstance): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteInstance.name)
    }

    async deleteAllInstances(input: DeleteAllInstances): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteAllInstances.name)
    }

    async deleteClass(input: DeleteClass): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteClass.name)
    }

    /**
     *
     * Terminate ProjectUser session (or all sessions)
     * @param {TerminateSession} input
     * @return {*}  {(Promise<GenerateCustomTokenResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async terminateSession(input: TerminateSession): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.terminateSession.name)
    }

    /**
     *
     * Generates custom user token which can be used to authenticate via Retter SDKs
     * @param {GenerateCustomToken} input
     * @return {*}  {(Promise<GenerateCustomTokenResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async generateCustomToken(input: GenerateCustomToken): Promise<GenerateCustomTokenResponse | undefined> {
        return this.sendSingleOperation(input, this.generateCustomToken.name)
    }

    /**
     *
     * Starts a bulk import operation in background
     * @param {BulkImport} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof OperationResponse
     */
    async bulkImport(input: BulkImport): Promise<BulkImportResponse | undefined> {
        return this.sendSingleOperation(input, this.bulkImport.name)
    }

    /**
     *
     * Makes a method call to an instance
     * @param {MethodCall} input
     * @return {*}  {(Promise<CloudObjectResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async methodCall(input: MethodCall): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.methodCall.name)
    }

    /**
     *
     * Creates a new instance or returns an existing one
     * @param {GetInstance} input
     * @return {*}  {(Promise<CloudObjectResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async getInstance(input: GetInstance): Promise<GetInstanceResponse | undefined> {
        return this.sendSingleOperation(input, this.getInstance.name)
    }

    async listInstanceIds(input: ListInstanceIds): Promise<ListInstanceIdsResponse | undefined> {
        return this.sendSingleOperation(input, this.listInstanceIds.name)
    }

    async listFiles(input: ListFiles): Promise<ListFilesResponse | undefined> {
        return this.sendSingleOperation(input, this.listFiles.name)
    }

    /**
     *
     * Gets the state of an instance
     * @param {GetInstance} input
     * @return {*}  {(Promise<CloudObjectResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async getState(input: GetInstance): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.getState.name)
    }

    /**
     *
     * Gets the instance id coressponds to the given lookup key
     * @param {LookUpKey} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async getLookUpKey(input: GetLookUpKey): Promise<GetLookupKeyResponse | undefined> {
        return this.sendSingleOperation(input, this.getLookUpKey.name)
    }

    /**
     *
     * Sets the current instance id to the given lookup key
     * @param {LookUpKey} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async setLookUpKey(input: LookUpKey): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.setLookUpKey.name)
    }

    /**
     *
     * Deletes the given lookup key
     * @param {LookUpKey} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async deleteLookUpKey(input: LookUpKey): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteLookUpKey.name)
    }

    /**
     *
     * Sets the value to the given key in memory
     * @param {SetMemory} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async setMemory(input: SetMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.setMemory.name)
    }

    /**
     *
     * Gets the value of the given key from memory
     * @param {GetMemory} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async getMemory(input: GetMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.getMemory.name)
    }

    /**
     *
     * Deletes the given key from memory
     * @param {DeleteMemory} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async deleteMemory(input: DeleteMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteMemory.name)
    }

    /**
     *
     * Increments the value of the given key in memory
     * 
     * The value have to be a number
     * Input parameter path can be used to increment a nested key.
     * @param {IncrementMemory} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async incrementMemory(input: IncrementMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.incrementMemory.name)
    }
    /**
     *
     * Gets file
     * @param {GetFile} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async getFile(input: GetFile): Promise<GetFileResponse | undefined> {
        const result = await this.sendSingleOperation(input, this.getFile.name)
        if (result.success && result.extraData?.url && !input.returnSignedURL) {
            return axios.get(result.extraData.url)
                .then((r) => ({
                    ...result,
                    extraData: undefined,
                    data: r.data
                }))
                .catch((e) => ({ success: false, error: e.message } as OperationResponse))
        }
        return result
    }

    /**
     *
     * Uploads file
     * @param {SetFile} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async setFile(input: SetFile): Promise<OperationResponse | undefined> {
        const size = calculateSize(input.body)

        const setFileOperation: SetFileOperation = {
            ...input,
            size,
            large: size > 5242880 //5mb
        }

        if (setFileOperation.large) {
            delete setFileOperation.body
        }
        let promise = this.sendSingleOperation(setFileOperation, this.setFile.name)
        if (setFileOperation.large) {
            promise = promise.then((r: OperationResponse) => {
                if (!r.success) return r
                return axios
                    .put(r.data.url, input.body, {
                        headers: {
                            'Content-Length': size.toString(),
                        },
                        maxBodyLength: fileSizeLimit,
                        maxContentLength: fileSizeLimit,
                    })
                    .then(() => ({ success: true } as OperationResponse))
                    .catch((e) => ({ success: false, error: e.message } as OperationResponse))
            })
        }
        return promise
    }

    /**
     *
     * Deletes file
     * @param {GetFile} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async deleteFile(input: GetFile): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteFile.name)
    }

    /**
     *
     * Deploys an existing class
     * @param {DeployProject} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async deployProject(input: DeployProject): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deployProject.name)
    }

    /**
     *
     * Upserts a dependency
     * @param {UpsertDependency} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async upsertDependency(input: UpsertDependency): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation({ ...input, commit: false, zipFile: undefined }, this.upsertDependency.name).then((r: OperationResponse) => {
            if (!r.success) return r
            return axios
                .put(r.data.url, input.zipFile, {
                    headers: {
                        'Content-Type': 'application/zip',
                    },
                    maxBodyLength: fileSizeLimit,
                    maxContentLength: fileSizeLimit,
                })
                .then(() =>
                    this.sendSingleOperation({ ...input, commit: true, zipFile: undefined }, this.upsertDependency.name).catch(
                        (e) => ({ success: false, error: e.message } as OperationResponse),
                    ),
                )
        })
    }

    /**
     *
     * Delete a dependency
     * @param {deleteDependency} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async deleteDependency(input: DeleteDependency): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteDependency.name)
    }

    /**
     *
     * Invalidates cache for given path
     * @param {InvalidateCache} input
     * @return {*}  {(Promise<InvalidateCacheResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async invalidateCache(input: InvalidateCache): Promise<InvalidateCacheResponse | undefined> {
        return this.sendSingleOperation(input, this.invalidateCache.name)
    }

    // * <database>
    async writeToDatabase(input: WriteToDatabase): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.writeToDatabase.name)
    }
    async incrementDatabase(input: IncrementDatabase): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.incrementDatabase.name)
    }
    async readDatabase(input: ReadDatabase): Promise<ReadDatabaseResponse | undefined> {
        return this.sendSingleOperation(input, this.readDatabase.name)
    }
    async removeFromDatabase(input: RemoveFromDatabase): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.removeFromDatabase.name)
    }
    async removePartitionFromDatabase(input: RemovePartitionFromDatabase): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.removePartitionFromDatabase.name)
    }
    async queryDatabase(input: QueryDatabase): Promise<QueryDatabaseResponse | undefined> {
        return this.sendSingleOperation(input, this.queryDatabase.name)
    }
    // * </database>

    async httpRequest(input: StaticIPRequest): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.httpRequest.name)
    }
}
export class CloudObjectsPipeline {
    private payload: OperationsInput = {}

    /**
     *
     * Generates custom user token which can be used to authenticate via Retter SDKs
     *
     * @param {GenerateCustomToken} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    generateCustomToken(input: GenerateCustomToken): CloudObjectsPipeline {
        if (!this.payload.generateCustomToken) this.payload.generateCustomToken = []
        this.payload.generateCustomToken.push(input)
        return this
    }

    deleteInstance(input: DeleteInstance): CloudObjectsPipeline {
        if (!this.payload.deleteInstance) this.payload.deleteInstance = []
        this.payload.deleteInstance.push(input)
        return this
    }

    deleteAllInstances(input: DeleteInstance): CloudObjectsPipeline {
        if (!this.payload.deleteAllInstances) this.payload.deleteAllInstances = []
        this.payload.deleteAllInstances.push(input)
        return this
    }

    deleteClass(input: DeleteClass): CloudObjectsPipeline {
        if (!this.payload.deleteClass) this.payload.deleteClass = []
        this.payload.deleteClass.push(input)
        return this
    }

    /**
     *
     * Gets the instance id coressponds to the given lookup key
     * @param {TerminateSession} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    terminateSession(input: TerminateSession): CloudObjectsPipeline {
        if (!this.payload.terminateSession) this.payload.terminateSession = []
        this.payload.terminateSession.push(input)
        return this
    }

    /**
     *
     * Gets the instance id coressponds to the given lookup key
     * @param {LookUpKey} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    getLookUpKey(input: GetLookUpKey): CloudObjectsPipeline {
        if (!this.payload.getLookUpKey) this.payload.getLookUpKey = []
        this.payload.getLookUpKey.push(input)
        return this
    }

    /**
     *
     * Sets the current instance id to the given lookup key
     * @param {LookUpKey} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    setLookUpKey(input: LookUpKey): CloudObjectsPipeline {
        if (!this.payload.setLookUpKey) this.payload.setLookUpKey = []
        this.payload.setLookUpKey.push(input)
        return this
    }

    /**
     *
     * Deletes the given lookup key
     * @param {LookUpKey} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    deleteLookUpKey(input: LookUpKey): CloudObjectsPipeline {
        if (!this.payload.deleteLookUpKey) this.payload.deleteLookUpKey = []
        this.payload.deleteLookUpKey.push(input)
        return this
    }

    /**
     *
     * Sets the value to the given key in memory
     * @param {SetMemory} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    setMemory(input: SetMemory): CloudObjectsPipeline {
        if (!this.payload.setMemory) this.payload.setMemory = []
        this.payload.setMemory.push(input)
        return this
    }

    /**
     *
     * Gets the value of the given key from memory
     * @param {GetMemory} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    getMemory(input: GetMemory): CloudObjectsPipeline {
        if (!this.payload.getMemory) this.payload.getMemory = []
        this.payload.getMemory.push(input)
        return this
    }

    /**
     *
     * Deletes the given key from memory
     * @param {DeleteMemory} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    deleteMemory(input: DeleteMemory): CloudObjectsPipeline {
        if (!this.payload.deleteMemory) this.payload.deleteMemory = []
        this.payload.deleteMemory.push(input)
        return this
    }

    /**
     *
     * Increments the value of the given key in memory
     *
     * The value have to be a number
     * Input parameter path can be used to increment a nested key.
     * @param {IncrementMemory} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    incrementMemory(input: IncrementMemory): CloudObjectsPipeline {
        if (!this.payload.incrementMemory) this.payload.incrementMemory = []
        this.payload.incrementMemory.push(input)
        return this
    }

    writeToDatabase(input: WriteToDatabase): CloudObjectsPipeline {
        if (!this.payload.writeToDatabase) this.payload.writeToDatabase = []
        this.payload.writeToDatabase.push(input)
        return this
    }

    readDatabase(input: ReadDatabase): CloudObjectsPipeline {
        if (!this.payload.readDatabase) this.payload.readDatabase = []
        this.payload.readDatabase.push(input)
        return this
    }

    removeFromDatabase(input: RemoveFromDatabase): CloudObjectsPipeline {
        if (!this.payload.removeFromDatabase) this.payload.removeFromDatabase = []
        this.payload.removeFromDatabase.push(input)
        return this
    }

    queryDatabase(input: QueryDatabase): CloudObjectsPipeline {
        if (!this.payload.queryDatabase) this.payload.queryDatabase = []
        this.payload.queryDatabase.push(input)
        return this
    }

    httpRequest(input: StaticIPRequest): CloudObjectsPipeline {
        if (!this.payload.httpRequest) this.payload.httpRequest = []
        this.payload.httpRequest.push(input)
        return this
    }

    /**
     *
     * Gets file
     * @param {GetFile} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    getFile(input: GetFile): CloudObjectsPipeline {
        if (!this.payload.getFile) this.payload.getFile = []
        this.payload.getFile.push(input)
        return this
    }

    /**
     *
     * Uploads file
     * @param {SetFile} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    setFile(input: SetFile): CloudObjectsPipeline {
        if (!this.payload.setFile) this.payload.setFile = []
        this.payload.setFile.push(input)
        return this
    }

    /**
     *
     * Deletes file
     * @param {GetFile} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    deleteFile(input: GetFile): CloudObjectsPipeline {
        if (!this.payload.deleteFile) this.payload.deleteFile = []
        this.payload.deleteFile.push(input)
        return this
    }

    /**
     *
     * Start a bulk import operation in background
     * @param {BulkImport} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    bulkImport(input: BulkImport): CloudObjectsPipeline {
        if (!this.payload.bulkImport) this.payload.bulkImport = []
        this.payload.bulkImport.push(input)
        return this
    }

    /**
     *
     * Makes a method call to an instance
     * @param {MethodCall} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    methodCall(input: MethodCall): CloudObjectsPipeline {
        if (!this.payload.methodCall) this.payload.methodCall = []
        this.payload.methodCall.push(input)
        return this
    }

    /**
     *
     * Creates a new instance or returns an existing one
     * @param {GetInstance} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    getInstance(input: GetInstance): CloudObjectsPipeline {
        if (!this.payload.getInstance) this.payload.getInstance = []
        this.payload.getInstance.push(input)
        return this
    }

    /**
     *
     * Gets the state of an instance
     * @param {GetInstance} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    getState(input: GetInstance): CloudObjectsPipeline {
        if (!this.payload.getState) this.payload.getState = []
        this.payload.getState.push(input)
        return this
    }

    /**
     *
     * Invalidates cache for given path
     * @param {InvalidateCache} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    invalidateCache(input: InvalidateCache): CloudObjectsPipeline {
        if (!this.payload.invalidateCache) this.payload.invalidateCache = []
        this.payload.invalidateCache.push(input)
        return this
    }

    /**
     *
     * Sends a batch request for operations gathered in the pipeline
     * @return {*}  {Promise<OperationsOutput>}
     * @memberof CloudObjectsPipeline
     */
    async send(): Promise<OperationsOutput> {
        const setFileOperations: SetFileOperation[] | undefined = this.payload.setFile?.map((s) => ({
            ...s,
            size: calculateSize(s.body!),
            large: false,
        }))
        const totalSize = setFileOperations?.reduce((sum, o) => sum + o.size, 0)
        const files: string[] = []
        const large = totalSize && totalSize > 5242880
        if (large) {
            setFileOperations?.forEach((s) => {
                files.push(s.body || '')
                s.large = true
                s.body = undefined
            })
        }

        if (setFileOperations) this.payload.setFile = setFileOperations

        let promise = callOperationApi(this.payload)
        if (large) {
            promise = promise.then((r) => {
                if (r instanceof Error) return r

                return Promise.all(
                    r.setFile!.map((r: OperationResponse, i) => {
                        if (!r.success) return r
                        return axios
                            .put(r.data.url, files[i], {
                                maxBodyLength: fileSizeLimit,
                                maxContentLength: fileSizeLimit,
                            })
                            .then(() => ({ success: true } as OperationResponse))
                            .catch((e) => ({ success: false, error: e.message } as OperationResponse))
                    }),
                ).then((setFile) => ({ ...r, setFile }))
            })
        }
        return promise.then((r) => {
            if (r instanceof Error)
                return Object.keys(this.payload).reduce((final, key) => {
                    final[key] = this.payload[key].map(() => ({ success: false, error: r.message }))
                    return final
                }, {})

            this.payload = {}
            if (r.getFile) {
                return Promise.all(
                    r.getFile.map((g, index) => {
                        const getFileInput = this.payload.getFile?.[index]
                        if (g.success && g.extraData?.url && !getFileInput?.returnSignedURL) {
                            return axios
                                .get(g.extraData.url)
                                .then((r) => ({
                                    ...g,
                                    extraData: undefined,
                                    data: r.data,
                                }))
                                .catch((e) => ({ success: false, error: e.message } as OperationResponse))
                        }
                        return g
                    }),
                ).then((getFile) => {
                    r.getFile = getFile
                    return r
                })
            }
            return r
        })
    }
}
