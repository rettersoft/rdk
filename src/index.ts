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
export interface OperationExtraResponse extends OperationResponse {
    extraData?: any
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
    sourceIP: string
    sessionId?: string
    clientOs?: string
    targetServiceIds?: string[]
    relatedUserId?: string
    pathParameters?: {
        path: string
        rule?: string
        params?: KeyValueString
    }
}

type UserState = { [userId: string]: { [key: string]: any } }
type RoleState = { [identity: string]: { [key: string]: any } }

export interface State<PUB = KeyValue, PRIV = KeyValue, USER = UserState, ROLE = RoleState> {
    public?: PUB
    private?: PRIV
    user?: USER
    role?: ROLE
}

export interface Schedule {
    classId?: string
    instanceId?: string
    payload?: any
    method: string
    after: number
    retryConfig?: RetryConfig
}
export interface Task {
    classId?: string
    instanceId?: string
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
export interface GetFromSortedSet {
    setName: string
    sortKey: string
}
export interface RemoveFromSortedSet {
    setName: string
    sortKey: string
}

export interface AddToSortedSet {
    setName: string
    sortKey: string
    data: Record<string, unknown>
}
export interface QuerySortedSet {
    setName: string
    beginsWith?: string
    greaterOrEqual?: string
    lessOrEqual?: string
    reverse?: boolean
    nextToken?: string
    limit?: number
}

export interface GetFile {
    filename: string
}
export interface DeployClass {
    classId: string
    force?: boolean
}
export interface InvalidateCache {
    classId?: string
    methodName?: string
    instanceId?: string
}
export type Architecture = 'arm64' | 'x86_64' | 'x86_64,arm64' | 'arm64,x86_64'
export type Runtime = 'nodejs12.x,nodejs14.x' | 'nodejs12.x' | 'nodejs14.x' | 'nodejs16.x' | 'nodejs14.x,nodejs12.x'
export interface UpsertDependency {
    dependencyName: string
    zipFile: Buffer
    architectures?: Architecture
    runtimes?: Runtime
}

export interface DeleteDependency {
    dependencyName: string
}

export interface SetFile extends GetFile {
    body: string
}
export interface SetFileOperation extends GetFile {
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

export interface InitResponse<O = any> {
    state?: State
    config?: Configuration
    response?: Response<O>
}


export interface GenerateCustomToken {
    userId: string
    identity: string
    claims?: KeyValue
}

export interface TerminateSession {
    userId: string
}

export interface DeleteInstance {
    classId: string
    instanceId: string
}

export interface ReadOnlyOperationsInput {
    getMemory?: GetMemory[]
    getFromSortedSet?: GetFromSortedSet[]
    querySortedSet?: QuerySortedSet[]
    readDatabase?: ReadDatabase[]
    queryDatabase?: QueryDatabase[]
    getFile?: GetFile[]
    getLookUpKey?: LookUpKey[]
    methodCall?: MethodCall[]
    getInstance?: GetInstance[]
    listInstanceIds?: ListInstanceIds[]
    listFiles?: ListFiles[]
    getState?: GetInstance[]
    generateCustomToken?: GenerateCustomToken[]
    request?: StaticIPRequest[]
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
export interface RemoveFromDatabase {
    partKey: string
    sortKey: string
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
    instanceId: string
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
}

export interface OperationsInput extends ReadOnlyOperationsInput {
    setMemory?: SetMemory[]
    deleteMemory?: GetMemory[]
    incrementMemory?: IncrementMemory[]
    addToSortedSet?: AddToSortedSet[]
    removeFromSortedSet?: GetFromSortedSet[]
    writeToDatabase?: WriteToDatabase[]
    removeFromDatabase?: RemoveFromDatabase[]
    setFile?: (SetFile | SetFileOperation)[]
    deleteFile?: GetFile[]
    setLookUpKey?: LookUpKey[]
    deleteLookUpKey?: LookUpKey[]
    upsertDependency?: UpsertDependency[]
    deleteDependency?: DeleteDependency[]
    deployClass?: DeployClass[]
    invalidateCache?: InvalidateCache[]
    terminateSession?: TerminateSession[]
    deleteInstance?: DeleteInstance[]
}

export interface ReadonlyOperationsOutput {
    getMemory?: OperationResponse[]
    getFromSortedSet?: OperationResponse[]
    querySortedSet?: OperationResponse[]
    readDatabase?: OperationResponse[]
    queryDatabase?: OperationResponse[]
    getFile?: OperationResponse[]
    getLookUpKey?: OperationResponse[]
    methodCall?: CloudObjectResponse[]
    getInstance?: CloudObjectResponse[]
    listInstanceIds?: CloudObjectResponse[]
    listFiles?: CloudObjectResponse[]
    getState?: CloudObjectResponse[]
    generateCustomToken?: GenerateCustomTokenResponse[]
    request?: OperationResponse[],
    httpRequest?: OperationResponse[],
    deleteInstance?: CloudObjectResponse[]
}

export interface OperationsOutput extends ReadonlyOperationsOutput {
    setMemory?: OperationResponse[]
    deleteMemory?: OperationResponse[]
    incrementMemory?: OperationResponse[]
    addToSortedSet?: OperationResponse[]
    removeFromSortedSet?: OperationResponse[]
    writeToDatabase?: OperationResponse[]
    removeFromDatabase?: OperationResponse[]
    setFile?: OperationResponse[]
    deleteFile?: OperationResponse[]
    setLookUpKey?: OperationResponse[]
    deleteLookUpKey?: OperationResponse[]
    upsertDependency?: OperationResponse[]
    deleteDependency?: OperationResponse[]
    deployClass?: OperationResponse[]
    invalidateCache?: InvalidateCacheResponse[]
    terminateSession?: TerminateSession[]
}

export interface StepResponse<T = any, PUB = KeyValue, PRIV = KeyValue, USER = UserState, ROLE = RoleState> {
    state?: State<PUB, PRIV, USER, ROLE>
    response?: Response<T>
    nextFlowId?: string
}

export interface RioEvent {
    name: string
    payload: Record<string, any>
}

export interface Data<I = any, O = any, PUB = KeyValue, PRIV = KeyValue, USER = UserState, ROLE = RoleState>
    extends StepResponse<O, PUB, PRIV, USER, ROLE> {
    context: Context
    env: KeyValue
    config: Configuration
    version: number
    state: State<PUB, PRIV, USER, ROLE>
    request: Request<I>
    response: Response<O>
    schedule: Schedule[]
    tasks: Task[]
    events: RioEvent[]
    nextFlowId?: string
}

import { Lambda } from 'aws-sdk'
let lambda: Lambda | undefined = undefined
let token = ''
let operationLambda = ''

const fileSizeLimit = 250000000


let operationCountMilestone = 0;
let concurrentLambdaCountLimit = 0;

let usageCheckCounter = 0;
let operationCount = 0
let concurrentLambdaCount = 0;
export function init(c: any, t: string, o: string, ocLimit?: number, clcLimit?: number) {
    lambda = new Lambda({ credentials: c })
    token = t
    operationLambda = o
    operationCountMilestone = ocLimit || 100
    concurrentLambdaCountLimit = clcLimit || 10
}

function calculateSize(data: string) {
    return new TextEncoder().encode(data).length
}
async function invokeLambda(payload: OperationsInput): Promise<OperationsOutput> {

    if (concurrentLambdaCount > concurrentLambdaCountLimit) {
        throw new Error(`Cannot send more than ${concurrentLambdaCountLimit} operations without pipeline`);
    }
    operationCount = Object.values(payload).reduce((total, op) => total + op.length, operationCount)

    const newUsageCheck = Math.floor(operationCount / operationCountMilestone)
    let checkUsage = false
    if (newUsageCheck > 0 && newUsageCheck !== usageCheckCounter) {
        usageCheckCounter = newUsageCheck
        checkUsage = true
    }

    concurrentLambdaCount++
    return lambda!
        .invoke({
            FunctionName: operationLambda,
            Payload: JSON.stringify({ data: payload, token, checkUsage }),
        })
        .promise()
        .then(({ FunctionError: e, Payload: response }) => {
            concurrentLambdaCount--
            if (e) throw new Error(e)
            const r = JSON.parse(response as string)
            const err = r.error || r.limitError
            if (err)
                throw new Error(err)
            delete r?.limitError
            delete r?.error
            return r as OperationsOutput
        })

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
        return invokeLambda({ [operationType]: [input] }).then((r) => r[operationType]?.pop())
    }

    /**
     *
     * Terminate ProjectUser session (or all sessions)
     * @param {DeleteInstance} input
     * @return {*}  {(Promise<GenerateCustomTokenResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
     async deleteInstance(input: DeleteInstance): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteInstance.name)
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
    async getInstance(input: GetInstance): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.getInstance.name)
    }

    async listInstanceIds(input: ListInstanceIds): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.listInstanceIds.name)
    }

    async listFiles(input: ListFiles): Promise<CloudObjectResponse | undefined> {
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
    async getLookUpKey(input: LookUpKey): Promise<OperationResponse | undefined> {
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
     * Adds a value to the given sorted set with the given sort key
     * @param {AddToSortedSet} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async addToSortedSet(input: AddToSortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.addToSortedSet.name)
    }
    /**
     *
     * Gets the value of a sort key from the given sorted set
     * @param {AddToSortedSet} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async getFromSortedSet(input: GetFromSortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.getFromSortedSet.name)
    }

    /**
     *
     * Removes the given sort key from the given sorted set
     * @param {RemoveFromSortedSet} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async removeFromSortedSet(input: RemoveFromSortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.removeFromSortedSet.name)
    }
    /**
     *
     * Queries the given sorted set
     * @param {QuerySortedSet} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async querySortedSet(input: QuerySortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.querySortedSet.name)
    }

    /**
     *
     * Gets file
     * @param {GetFile} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async getFile(input: GetFile): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.getFile.name).then((g) => {
            if (g.success && g.extraData?.url) {
                return axios.get(g.extraData.url)
                    .then((r) => ({
                        ...g,
                        extraData: undefined,
                        data: r.data
                    }))
                    .catch((e) => ({ success: false, error: e.message } as OperationResponse))
            }
            return g
        })
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
                console.log("setFileReturn: ", JSON.stringify(r))
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
     * @param {DeployClass} input
     * @return {*}  {(Promise<OperationResponse | undefined>)}
     * @memberof CloudObjectsOperator
     */
    async deployClass(input: DeployClass): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deployClass.name)
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
    async readDatabase(input: ReadDatabase): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.readDatabase.name)
    }
    async removeFromDatabase(input: RemoveFromDatabase): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.removeFromDatabase.name)
    }
    async queryDatabase(input: QueryDatabase): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.queryDatabase.name)
    }
    // * </database>

    async request(input: StaticIPRequest): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.request.name)
    }

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

    /**
     *
     * Gets the instance id coressponds to the given lookup key
     * @param {TerminateSession} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
         deleteInstance(input: DeleteInstance): CloudObjectsPipeline {
            if (!this.payload.deleteInstance) this.payload.deleteInstance = []
            this.payload.deleteInstance.push(input)
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
    getLookUpKey(input: LookUpKey): CloudObjectsPipeline {
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

    /**
     *
     * Adds a value to the given sorted set with the given sort key
     * @param {AddToSortedSet} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    addToSortedSet(input: AddToSortedSet): CloudObjectsPipeline {
        if (!this.payload.addToSortedSet) this.payload.addToSortedSet = []
        this.payload.addToSortedSet.push(input)
        return this
    }

    /**
     *
     * Gets the value of a sort key from the given sorted set
     * @param {AddToSortedSet} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    getFromSortedSet(input: GetFromSortedSet): CloudObjectsPipeline {
        if (!this.payload.getFromSortedSet) this.payload.getFromSortedSet = []
        this.payload.getFromSortedSet.push(input)
        return this
    }

    /**
     *
     * Removes the given sort key from the given sorted set
     * @param {RemoveFromSortedSet} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    removeFromSortedSet(input: RemoveFromSortedSet): CloudObjectsPipeline {
        if (!this.payload.removeFromSortedSet) this.payload.removeFromSortedSet = []
        this.payload.removeFromSortedSet.push(input)
        return this
    }
    /**
     *
     * Queries the given sorted set
     * @param {QuerySortedSet} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    querySortedSet(input: QuerySortedSet): CloudObjectsPipeline {
        if (!this.payload.querySortedSet) this.payload.querySortedSet = []
        this.payload.querySortedSet.push(input)
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

    request(input: StaticIPRequest): CloudObjectsPipeline {
        if (!this.payload.request) this.payload.request = []
        this.payload.request.push(input)
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
     * Deploys an existing class
     * @param {DeployClass} input
     * @return {*}  {CloudObjectsPipeline}
     * @memberof CloudObjectsPipeline
     */
    deployClass(input: DeployClass): CloudObjectsPipeline {
        if (!this.payload.deployClass) this.payload.deployClass = []
        this.payload.deployClass.push(input)
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
            large: false
        }))
        const totalSize = setFileOperations?.reduce((sum, o) => sum + o.size, 0)
        const large = totalSize && totalSize > 5242880
        if (large) {
            setFileOperations?.forEach((s) => {
                s.large = true;
                s.body = undefined;
            })
        }

        if (setFileOperations) {
            this.payload.setFile = setFileOperations
        }

        let promise = invokeLambda(this.payload)
        if (large) {
            promise = promise.then((r) => Promise.all(r.setFile!.map((r: OperationResponse, i) => {
                console.log("setFilePipelineReturn: ", JSON.stringify(r))
                if (!r.success) return r
                const { body } = this.payload.setFile![i]
                return axios
                    .put(r.data.url, body, {
                        maxBodyLength: fileSizeLimit,
                        maxContentLength: fileSizeLimit,
                    })
                    .then(() => ({ success: true } as OperationResponse))
                    .catch((e) => ({ success: false, error: e.message } as OperationResponse))
            })).then((setFile) => ({ ...r, setFile })))
        }
        return promise.then((r) => {
            this.payload = {}
            if (r.getFile) {
                return Promise.all((r.getFile as OperationExtraResponse[]).map((g) => {
                    if (g.success && g.extraData?.url) {
                        return axios.get(g.extraData.url).then((r) => ({
                            ...g,
                            extraData: undefined,
                            data: r.data
                        }))
                            .catch((e) => ({ success: false, error: e.message } as OperationResponse))
                    }
                    return g

                })).then((getFile) => {
                    r.getFile = getFile
                    return r
                })
            }
            return r
        })
    }
}
