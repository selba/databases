Public Module Comu1

    ''' <summary>Devuelve la versión del módulo solictado</summary>
    ''' <returns>La versión de la librería</returns>
    Public Function GetLibraryVersion() As String
        GetLibraryVersion = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString()
    End Function

End Module
