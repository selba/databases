Imports System.Windows.Forms
Imports Microsoft.SqlServer.Management.Smo

Namespace SqlServer
    ''' <summary>
    ''' DBMS: Data Bades Management Sytem
    ''' Sistema Gestor de Bases de Datos
    ''' Controla el Acceso sobre una BD de SqlServer
    ''' </summary>
    ''' <remarks></remarks>
    Public Class SQLSeverDBMS
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
        Private _Cn As New SqlClient.SqlConnection
        Private _DbName As String
        Private _DataSource, _User, _Password As String
        Private _DataAdapters As New Generic.Dictionary(Of String, SqlClient.SqlDataAdapter)
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
        Public Property DbName() As String
            Get
                Return (_DbName)
            End Get
            Set(ByVal value As String)
                _DbName = value
            End Set
        End Property
        ''' <summary>
        ''' Establece/Recupera el DataSource de la BD
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property DataSource() As String
            Get
                Return (_DataSource)
            End Get
            Set(ByVal value As String)
                _DataSource = value
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

        Public ReadOnly Property GetConnection() As SqlClient.SqlConnection
            Get
                Dim Cn As New SqlClient.SqlConnection

                If _DataSource = "" Then _DataSource = "(local)\SQLEXPRESS"
                If _UseWindowsAuthentication Then
                    Cn.ConnectionString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=" + _DbName + ";Data Source=" + DataSource
                Else
                    If _Password <> "" Then
                        Cn.ConnectionString = "Data Source=" + DataSource + ";Initial Catalog=" + _DbName + ";User ID=" + _User + ";Password=" + Password
                    Else
                        Cn.ConnectionString = "Data Source=" + DataSource + ";Initial Catalog=" + _DbName + ";User ID=" + _User
                    End If
                End If
                Cn.ConnectionString += ";Connect Timeout=" & ConnectionTimeOut.ToString
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

        Public Property UseWindowsAuthentication() As Boolean
            Get
                Return _UseWindowsAuthentication
            End Get
            Set(ByVal value As Boolean)
                _UseWindowsAuthentication = value
            End Set
        End Property
#End Region
#Region "METODOS PUBLICOS"
        Public Sub BulkTable(ByVal SrcTable As DataTable, ByVal TableName As String)
            Dim Bulk As SqlClient.SqlBulkCopy
            Bulk = New SqlClient.SqlBulkCopy(GetConnection.ConnectionString, SqlClient.SqlBulkCopyOptions.TableLock)
            Bulk.DestinationTableName = TableName
            Bulk.WriteToServer(SrcTable)
        End Sub

        ''' <summary>
        ''' Obtiene un DataReader a partir de un comando SQL de tipo SELECT
        ''' </summary>
        ''' <param name="Sql"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function GetDataReader(ByVal Sql As String) As SqlClient.SqlDataReader
            Dim Cmd As New SqlClient.SqlCommand
            Dim Dr As SqlClient.SqlDataReader

            Cmd.Connection = GetConnection
            Cmd.CommandText = Sql
            Cmd.Connection.Open()
            Try
                Dr = Cmd.ExecuteReader(CommandBehavior.CloseConnection)
                Return (Dr)
            Catch e As Exception
                ' Throw e
                If PopupErrors Then
                    MessageBox.Show("[DB]Error en la ejecución del comando SQL: " + vbCrLf + Sql + " " + e.Message)
                End If
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
            Dim Cmd As New SqlClient.SqlCommand
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
                If PopupErrors Then
                    MessageBox.Show("[DB]Error en la ejecución del comando SQL: " + vbCrLf + Sql + " " + e.Message)
                End If
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
            Dim Dr As SqlClient.SqlDataReader
            Dim Table As New DataTable
            Dim StructTable As DataTable
            Dim Adaptador As SqlClient.SqlDataAdapter

            Select Case Mode
                Case ACCESS_TABLE_TYPE.ADD_ONLY
                    Dr = GetDataReader("Select top 1 * from " & TableName)
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
                    Adaptador = New SqlClient.SqlDataAdapter("Select * from " & TableName, _Cn.ConnectionString)
                    Adaptador.InsertCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_INSERT, StructTable)
                    Adaptador.UpdateCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_UPDATE, StructTable)
                    Adaptador.DeleteCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_DELETE, StructTable)
                    _DataAdapters.Add(TableName, Adaptador)
                End Try
            End If

            Return (Table)
        End Function
        Public Function UpdateTable(ByVal Table As DataTable) As DataTable
            Dim Adaptador As SqlClient.SqlDataAdapter
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
            Dim Dr As SqlClient.SqlDataReader = Nothing
            Dim Table As New DataTable
            Try
                Dr = GetDataReader(SqlText)
            Catch e As Exception
                'Throw e
                If PopupErrors Then
                    MessageBox.Show("[DB]Error en la ejecución del comando SQL: " + SqlText + " " + vbCrLf + e.Message)
                End If
            End Try
            Table.Load(Dr)
            Table.TableName = TableName
            Return (Table)
        End Function

        Public Function CreateAdapter(ByVal TableName As String) As SqlClient.SqlDataAdapter
            Dim Dr As SqlClient.SqlDataReader
            Dim Table As New DataTable
            Dim StructTable As DataTable
            Dim Adaptador As SqlClient.SqlDataAdapter

            Dr = GetDataReader("Select top 1 * from " & TableName)
            Table.Load(Dr)
            Table.TableName = TableName
            Dr.Close()
            Try
                Adaptador = _DataAdapters(TableName) 'Comprueba si existe

            Catch ex As KeyNotFoundException
                StructTable = GetSchemaTable(TableName)
                Adaptador = New SqlClient.SqlDataAdapter("Select * from " & TableName, _Cn.ConnectionString)
                Adaptador.InsertCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_INSERT, StructTable)
                Adaptador.UpdateCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_UPDATE, StructTable)
                Adaptador.DeleteCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_DELETE, StructTable)
                _DataAdapters.Add(TableName, Adaptador)
            End Try
            Return Adaptador
        End Function
        ''' <summary>
        ''' Ejecuta un comando SQL de Acción
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function AttachDB(ByVal DbName As String, ByVal FileData As String, ByVal FileLog As String) As Boolean
            Dim Cmd As New SqlClient.SqlCommand
            Dim AffectedRows As Int32
            Dim TmpDbName As String = _DbName
            _DbName = "Master"
            Cmd.Connection = GetConnection
            Cmd.CommandText = "EXEC sp_attach_db @dbname = '" & DbName & "', @filename1 = '" + FileData + "', @filename2 = '" + FileLog + "'"
            Cmd.CommandType = CommandType.Text
            Cmd.Connection.Open()
            Try
                AffectedRows = Cmd.ExecuteNonQuery()
                Cmd.Connection.Close()
                _DbName = TmpDbName
                Return (True)
            Catch e As Exception
                If PopupErrors Then
                    MessageBox.Show("[DB]Error enlazando la base de datos " + e.Message)
                End If
                Cmd.Connection.Close()
                _DbName = TmpDbName
                Return False
                Exit Function
            End Try
        End Function
        ''' <summary>
        ''' Ejecuta un comando SQL de Acción
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function DetachDB(ByVal DbName As String) As Boolean
            Dim Cmd As New SqlClient.SqlCommand
            Dim AffectedRows As Int32
            Dim TmpDbName As String = _DbName

            _DbName = "Master"
            Cmd.Connection = GetConnection
            Cmd.CommandText = "ALTER DATABASE [" + DbName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;"
            Cmd.Connection.Open()
            Try
                AffectedRows = Cmd.ExecuteNonQuery()
                Cmd.CommandText = "sp_detach_db '" & DbName & "';"
                AffectedRows = Cmd.ExecuteNonQuery()
                Cmd.Connection.Close()
                _DbName = TmpDbName
                Return (True)
            Catch e As Exception
                If PopupErrors Then
                    MessageBox.Show("[DB]Error separando la base de datos " + e.Message)
                End If
                Cmd.Connection.Close()
                _DbName = TmpDbName
                Return False
                Exit Function
            End Try

        End Function

        Public Sub CloseConnection()
            For Each i As KeyValuePair(Of String, SqlClient.SqlDataAdapter) In _DataAdapters
                i.Value.Dispose()
            Next
        End Sub
        ''' <summary>
        ''' Retorna una tabla con todas las tablas enlazadas en SQLServer
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function GetListAttachedDb() As DataTable
            Dim TmpDbName As String = _DbName
            _DbName = "Master"
            Dim Dr As SqlClient.SqlDataReader = Nothing
            Dim Table As New DataTable
            Try
                Dr = GetDataReader("Select name from sysdatabases")
            Catch e As Exception
                'Throw e
                If PopupErrors Then
                    MessageBox.Show("[DB]Error en la ejecución del comando SQL: " + "Select name from sysdatabases" + " " + vbCrLf + e.Message)
                End If
            Finally
                _DbName = TmpDbName
            End Try
            Table.Load(Dr)
            Table.TableName = "DataBases"
            Return (Table)
        End Function
        Public Sub ShrinkDb(ByVal DbName As String, ByVal TimeOut As Integer)

            DoSQLAction("DBCC SHRINKDATABASE (" + DbName + ")", TimeOut) '4 minutos de tiempo

            'Dim srv As Server
            'srv = New Server(_DataSource)

            'Reference the AdventureWorks database.
            'Dim db As Database
            'db = srv.Databases(DbName)

            'Shrink the database without truncating the log.
            'db.Shrink(Percent, ShrinkMethod.Default)

            'Truncate the log.
            'db.TruncateLog()
        End Sub
        Public Sub ReIndexTable(ByVal TableName As String, ByVal TimeOut As Integer)
            DoSQLAction("DBCC DBREINDEX (" + TableName + ", '', 0)", TimeoUt)
        End Sub
        Public Sub SetAccess(ByVal Mode As DB_ACCESS_MODES)
            If Mode = DB_ACCESS_MODES.READ_ONLY Then
                DoSQLAction("ALTER DATABASE [" + DbName + "] SET READ_ONLY")
            Else
                DoSQLAction("ALTER DATABASE [" + DbName + "] SET READ_WRITE")
            End If
        End Sub
#End Region
#Region "METODOS PRIVADOS"
        Private Function GetSchemaTable(ByVal TableName As String) As DataTable
            Dim Cmd As New SqlClient.SqlCommand
            Dim Dr As SqlClient.SqlDataReader
            Dim Schema As DataTable

            Cmd.Connection = GetConnection
            Cmd.CommandText = "Select top 1 * from " & TableName
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

        Private Function DoSqlCommand(ByVal Table As DataTable, ByVal SqlType As SQL_COMMAND_TYPE, ByVal StructTable As DataTable) As SqlClient.SqlCommand
            Dim Command As New SqlClient.SqlCommand
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
                        If CType(StructTable.Rows(i).Item("IsAutoIncrement"), Boolean) = False Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= "[" & Table.Columns(i).ColumnName & "]"
                            Else
                                Command.CommandText &= "," & "[" & Table.Columns(i).ColumnName & "]"
                            End If
                            Dim Parameter As New SqlClient.SqlParameter
                            Parameter.ParameterName = "@Parameter" & (i + 1).ToString ' & "[" & Table.Columns(i).ColumnName & "]"
                            Parameter.SourceColumn = Table.Columns(i).ColumnName
                            Command.Parameters.Add(Parameter)
                        End If
                    Next
                    Command.CommandText &= ")values("
                    FirstField = True
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("IsAutoIncrement"), Boolean) = False Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= "@Parameter" & (i + 1).ToString '"@" & "[" & Table.Columns(i).ColumnName & "]"
                            Else
                                Command.CommandText &= ",@Parameter" & (i + 1).ToString '",@" & "[" & Table.Columns(i).ColumnName & "]"
                            End If
                        End If
                    Next
                    Command.CommandText &= ")"
                Case SQL_COMMAND_TYPE.SQL_UPDATE
                    Command.CommandText = "Update " & Table.TableName & " Set "
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("Iskey"), Boolean) = False And _
                        CType(StructTable.Rows(i).Item("IsAutoIncrement"), Boolean) = False Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= "[" & Table.Columns(i).ColumnName & "]" & "=@Parameter" & (i + 1).ToString '"=@" & Table.Columns(i).ColumnName
                            Else
                                Command.CommandText &= ",[" & Table.Columns(i).ColumnName & "]" & "=@Parameter" & (i + 1).ToString '=@" & Table.Columns(i).ColumnName
                            End If
                            Dim Parameter As New SqlClient.SqlParameter
                            Parameter.ParameterName = "@Parameter" & (i + 1).Tostring ' & Table.Columns(i).ColumnName
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
                                Command.CommandText &= "[" & Table.Columns(i).ColumnName & "]" & "=@Parameter" & (i + 1).tostring ' & Table.Columns(i).ColumnName
                            Else
                                Command.CommandText &= " and " & "[" & Table.Columns(i).ColumnName & "]=@Parameter" & (i + 1).tostring '@" & Table.Columns(i).ColumnName
                            End If
                            Dim Parameter As New SqlClient.SqlParameter
                            Parameter.ParameterName = "@Parameter" & (i + 1).Tostring '"@" & Table.Columns(i).ColumnName
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
                                Command.CommandText &= "[" & Table.Columns(i).ColumnName & "]=@Parameter" & (i + 1).tostring '@" & Table.Columns(i).ColumnName
                            Else
                                Command.CommandText &= " and " & "[" & Table.Columns(i).ColumnName & "]=@Parameter" & (i + 1).tostring '=@" & Table.Columns(i).ColumnName
                            End If
                            Dim Parameter As New SqlClient.SqlParameter
                            Parameter.ParameterName = "@Parameter" & (i + 1).Tostring '"@" & Table.Columns(i).ColumnName
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
