Imports System.Data.OracleClient

Namespace OracleNetClient
    Public Class OracleDBMS
#Region "ESTRUCTURAS"
        Enum ACCESS_TABLE_TYPE
            READ_ONLY = 1
            ADD_ONLY = 2
            READ_WRITE = 4
        End Enum
        Private Enum SQL_COMMAND_TYPE
            SQL_SELECT = 0
            SQL_INSERT = 1
            SQL_UPDATE = 2
            SQL_DELETE = 3
        End Enum
        Enum DB_ACCESS_MODES
            READ_WRITE = 0
            READ_ONLY = 1
        End Enum

#End Region
#Region "VARIABLES"
        Private _Cn As New OracleConnection
        Private _ConnectionName As String
        Private _DataSource, _User, _Password As String
        Private _DataAdapters As New Generic.Dictionary(Of String, OracleDataAdapter)
        Private _PopUpErrors As Boolean
        Private _UseWindowsAuthentication As Boolean
        Private _ConnetionTimeOut As Integer = 15
#End Region
#Region "PROPIEDADES"

        ''' <summary>
        ''' Establece/Recupera el nombre de la base de datos activa(Catalogo)
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property ConnectionName() As String
            Get
                Return (_ConnectionName)
            End Get
            Set(ByVal value As String)
                _ConnectionName = value
            End Set
        End Property
        ''' <summary>
        ''' Establece/Recupera el Usuario de la BD
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property User() As String
            Get
                Return (_User)
            End Get
            Set(ByVal value As String)
                _User = value
            End Set
        End Property

        ''' <summary>
        ''' Establece/Recupera el Password de la BD
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property Password() As String
            Get
                Return (_Password)
            End Get
            Set(ByVal value As String)
                _Password = value
            End Set
        End Property

        Public Property ConnectionTimeOut() As Integer
            Get
                Return _ConnetionTimeOut
            End Get
            Set(ByVal value As Integer)
                _ConnetionTimeOut = value
            End Set
        End Property

        Public ReadOnly Property GetConnection() As OracleConnection
            Get
                Dim Cn As New OracleConnection
                If ExplicitConnectionString = "" Then
                    Cn.ConnectionString = "DATA SOURCE=" + ConnectionName + ";USER ID=" + User + ";PASSWORD=" & Password & ";"
                    Cn.ConnectionString += ";Connect Timeout=" & ConnectionTimeOut.ToString
                Else
                    Cn.ConnectionString = ExplicitConnectionString
                End If
                Return (Cn)

            End Get
        End Property
        ''' <summary>
        ''' Indica si pot mostrar messagebox quan detecti errors
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property PopupErrors() As Boolean
            Get
                Return _PopUpErrors
            End Get
            Set(ByVal value As Boolean)
                _PopUpErrors = value
            End Set
        End Property

        Public Property ExplicitConnectionString As String = ""

#End Region

#Region "Public Methods"
        
        'Public Sub BulkTable(ByVal SrcTable As DataTable, ByVal TableName As String)
        '    Dim Bulk As OracleBulkCopy
        '    Bulk = New OracleBulkCopy(GetConnection.ConnectionString, OracleBulkCopyOptions.Default)
        '    Bulk.DestinationTableName = TableName
        '    Bulk.WriteToServer(SrcTable)
        'End Sub
        ''' <summary>
        ''' Obtiene un DataReader a partir de un comando SQL de tipo SELECT
        ''' </summary>
        ''' <param name="Sql"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function GetDataReader(ByVal Sql As String) As OracleDataReader
            Dim Cmd As New OracleCommand
            Dim Dr As OracleDataReader

            Cmd.Connection = GetConnection
            Cmd.CommandText = Sql

            Try
                Cmd.Connection.Open()
                Dr = Cmd.ExecuteReader(CommandBehavior.CloseConnection)
                Return (Dr)
            Catch e As Exception
                Return (Nothing)
                Exit Function
            End Try
        End Function

        ''' <summary>
        ''' Ejecuta un comando SQL de Acción
        ''' </summary>
        ''' <param name="Sql"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function DoSQLAction(ByVal Sql As String, Optional ByVal TimeOut As Integer = 30) As Int32
            Dim Cmd As New OracleCommand
            Dim AffectedRows As Int32

            Cmd.Connection = GetConnection
            Cmd.CommandText = Sql
            Cmd.CommandTimeout = TimeOut
            Cmd.Connection.Open()
            Try
                AffectedRows = Cmd.ExecuteNonQuery()
                Cmd.Connection.Close()
                Return (AffectedRows)
            Catch e As Exception
                Cmd.Connection.Close()
                Return (0)
                Exit Function
            End Try
        End Function
        ''' <summary>
        ''' Devuelve una Tabla
        ''' Si se solicita de read/write crea (Si no existe) el DataAdapter correspondiente        ''' 
        ''' </summary>
        ''' <param name="TableName">Nombre de la Tabla o sentencia SQL si acceso READ_ONLY_USING_SQL</param>
        ''' <param name="Mode">Tipo de Acceso</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function GetTable(ByVal TableName As String, ByVal Mode As ACCESS_TABLE_TYPE) As DataTable
            Dim Dr As OracleDataReader
            Dim Table As New DataTable
            Dim StructTable As DataTable
            Dim Adaptador As OracleDataAdapter

            Select Case Mode
                Case ACCESS_TABLE_TYPE.ADD_ONLY
                    Dr = GetDataReader("Select * from " & TableName & " where rowindex<2")
                Case Else
                    Dr = GetDataReader("Select * from " & TableName)
            End Select

            Table.Load(Dr)
            Table.TableName = TableName


            'Si es lectura escritura debe crear el correspondiente dataadapter
            If Mode = ACCESS_TABLE_TYPE.READ_WRITE Or Mode = ACCESS_TABLE_TYPE.ADD_ONLY Then
                Try
                    Adaptador = _DataAdapters(TableName) 'Comprueba si existe

                Catch ex As KeyNotFoundException
                    StructTable = GetSchemaTable(TableName)
                    Adaptador = New OracleDataAdapter("Select * from " & TableName, _Cn.ConnectionString)
                    Adaptador.InsertCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_INSERT, StructTable)
                    Adaptador.UpdateCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_UPDATE, StructTable)
                    Adaptador.DeleteCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_DELETE, StructTable)
                    _DataAdapters.Add(TableName, Adaptador)
                End Try
            End If

            Return (Table)
        End Function

        ''' <summary>
        ''' Aplica los cambios a la BD
        ''' </summary>
        ''' <param name="Table"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function UpdateTable(ByVal Table As DataTable) As DataTable
            Dim Adaptador As OracleDataAdapter
            Dim ChangesTable As DataTable
            Try
                Adaptador = _DataAdapters(Table.TableName) 'Comprueba si existe
                ChangesTable = Table.GetChanges()
                If ChangesTable IsNot Nothing Then
                    Adaptador.Update(ChangesTable)
                End If
                Table.AcceptChanges()
                Return (Table)
            Catch ex As KeyNotFoundException
                Return (Nothing)
            End Try
        End Function

        ''' <summary>
        ''' Devuelve una Tabla a partir de una sentencia SQL
        ''' </summary>
        ''' <param name="SqlText">Sentencia SQL</param>
        ''' <param name="TableName">Nombre de la Tabla de Retorno</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function GetSqlTable(ByVal SqlText As String, ByVal TableName As String) As DataTable
            Dim Dr As OracleDataReader = Nothing
            Dim Table As New DataTable
            Try
                Dr = GetDataReader(SqlText)
            Catch e As Exception
            End Try
            Table.Load(Dr)
            Table.TableName = TableName
            Return (Table)
        End Function

        ''' <summary>
        ''' Crea un adapter ara tabla indicada
        ''' </summary>
        ''' <param name="TableName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function CreateAdapter(ByVal TableName As String) As OracleDataAdapter
            Dim Dr As OracleDataReader
            Dim Table As New DataTable
            Dim StructTable As DataTable
            Dim Adaptador As OracleDataAdapter

            Dr = GetDataReader("Select * from " & TableName & " where rownum<2")
            Table.Load(Dr)
            Table.TableName = TableName
            Dr.Close()
            Try
                Adaptador = _DataAdapters(TableName) 'Comprueba si existe

            Catch ex As KeyNotFoundException
                StructTable = GetSchemaTable(TableName)
                Adaptador = New OracleDataAdapter("Select * from " & TableName, _Cn.ConnectionString)
                Adaptador.InsertCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_INSERT, StructTable)
                Adaptador.UpdateCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_UPDATE, StructTable)
                Adaptador.DeleteCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_DELETE, StructTable)
                _DataAdapters.Add(TableName, Adaptador)
            End Try
            Return Adaptador
        End Function

        ''' <summary>
        ''' Cierra las posibles conexiones de Adapters
        ''' </summary>
        ''' <remarks></remarks>
        Public Sub CloseConnection()
            For Each i As KeyValuePair(Of String, OracleDataAdapter) In _DataAdapters
                i.Value.Dispose()
            Next
        End Sub

#End Region

#Region "Private Methods"
        ''' <summary>
        ''' Obtiene la información de esqeuma de la tabla: Campos, tipos, longitudes,...
        ''' </summary>
        ''' <param name="TableName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function GetSchemaTable(ByVal TableName As String) As DataTable
            Dim Cmd As New OracleCommand
            Dim Dr As OracleDataReader
            Dim Schema As DataTable

            Cmd.Connection = GetConnection
            Cmd.CommandText = "Select * from " & TableName & " where rownum<2"
            Cmd.Connection.Open()

            Try
                Dr = Cmd.ExecuteReader(CommandBehavior.KeyInfo)
                Schema = Dr.GetSchemaTable
                Cmd.Connection.Close()
                Return (Schema)
            Catch e As Exception
                Return (Nothing)
                Exit Function
            End Try
        End Function
        ''' <summary>
        ''' Genera los Commands INSERT,DELETE y UPDATE para el Adapter.
        ''' </summary>
        ''' <param name="Table"></param>
        ''' <param name="SqlType"></param>
        ''' <param name="StructTable"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function DoSqlCommand(ByVal Table As DataTable, ByVal SqlType As SQL_COMMAND_TYPE, ByVal StructTable As DataTable) As OracleCommand
            Dim Command As New OracleCommand
            Dim i As Int32, FirstField As Boolean

            Command.CommandType = CommandType.Text
            Command.Connection = GetConnection
            FirstField = True
            Select Case SqlType
                Case SQL_COMMAND_TYPE.SQL_SELECT
                    Command.CommandText = "Select * from " & Table.TableName
                Case SQL_COMMAND_TYPE.SQL_INSERT
                    Command.CommandText = "Insert into " & Table.TableName & "("
                    For i = 0 To Table.Columns.Count - 1
                        'If CType(StructTable.Rows(i).Item("IsRowId"), Boolean) = False Then
                        If FirstField Then
                            FirstField = False
                            Command.CommandText &= Table.Columns(i).ColumnName
                        Else
                            Command.CommandText &= "," & Table.Columns(i).ColumnName
                        End If
                        Dim Parameter As New OracleParameter
                        Parameter.ParameterName = "p" & Table.Columns(i).ColumnName
                        Parameter.SourceColumn = Table.Columns(i).ColumnName
                        Command.Parameters.Add(Parameter)
                        'End If
                    Next
                    Command.CommandText &= ")values("
                    FirstField = True
                    For i = 0 To Table.Columns.Count - 1
                        'If CType(StructTable.Rows(i).Item("IsRowId"), Boolean) = False Then
                        If FirstField Then
                            FirstField = False
                            Command.CommandText &= ":p" & Table.Columns(i).ColumnName
                        Else
                            Command.CommandText &= ",:p" & Table.Columns(i).ColumnName
                        End If
                        'End If
                    Next
                    Command.CommandText &= ")"
                Case SQL_COMMAND_TYPE.SQL_UPDATE
                    Command.CommandText = "Update " & Table.TableName & " Set "
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("Iskey"), Boolean) = False Then 'And _
                            'CType(StructTable.Rows(i).Item("IsRowId"), Boolean) = False Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= Table.Columns(i).ColumnName & "=:p" & Table.Columns(i).ColumnName
                            Else
                                Command.CommandText &= "," & Table.Columns(i).ColumnName & "=:p" & Table.Columns(i).ColumnName
                            End If
                            Dim Parameter As New OracleParameter
                            Parameter.ParameterName = "p" & Table.Columns(i).ColumnName
                            Parameter.SourceColumn = Table.Columns(i).ColumnName
                            Command.Parameters.Add(Parameter)
                        End If
                    Next
                    Command.CommandText &= " where "
                    FirstField = True
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("IsKey"), Boolean) = True Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= Table.Columns(i).ColumnName & "=:p" & Table.Columns(i).ColumnName
                            Else
                                Command.CommandText &= " and " & Table.Columns(i).ColumnName & "=:p" & Table.Columns(i).ColumnName
                            End If
                            Dim Parameter As New OracleParameter
                            Parameter.ParameterName = "p" & Table.Columns(i).ColumnName
                            Parameter.SourceColumn = Table.Columns(i).ColumnName
                            Command.Parameters.Add(Parameter)
                        End If
                    Next
                Case SQL_COMMAND_TYPE.SQL_DELETE
                    Command.CommandText = "Delete from " & Table.TableName & " where "
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("IsKey"), Boolean) = True Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= Table.Columns(i).ColumnName & "=:p" & Table.Columns(i).ColumnName
                            Else
                                Command.CommandText &= " and " & Table.Columns(i).ColumnName & "=:p" & Table.Columns(i).ColumnName
                            End If
                            Dim Parameter As New OracleParameter
                            Parameter.ParameterName = "p" & Table.Columns(i).ColumnName
                            Parameter.SourceColumn = Table.Columns(i).ColumnName
                            Command.Parameters.Add(Parameter)
                        End If
                    Next
            End Select
            Return Command
        End Function
#End Region
    End Class

End Namespace
